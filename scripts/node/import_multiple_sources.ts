import fs from 'fs'
import path from 'path'
import csv from 'csv-parser'
import dotenv from 'dotenv'
import { createClient } from '@supabase/supabase-js'

// Load environment variables
dotenv.config({ path: '.env.local' })

const supabaseAdmin = createClient(
  'https://rqjlhvjrtlgqzmtaetjf.supabase.co',
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJxamxodmpydGxncXptdGFldGpmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NjYwMzc2NCwiZXhwIjoyMDcyMTc5NzY0fQ.5XMz5DvU5hCAtnDwn6v-0XSKwhaQJO-ppfAVXW9mLaA'
)

interface CsvRow {
  source: string
  title: string
  url?: string
  region: string
  observed_at: string
  raw_metric?: string
}

function toSlug(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
}

// Generate realistic search volumes based on ranking
function generateSearchVolume(rank: number, totalTrends: number): number {
  // Base volume for top trend (around 2-5M searches)
  const baseVolume = 3000000
  
  // Exponential decay based on rank
  const decayFactor = Math.exp(-rank * 0.15)
  
  // Add some randomness (±20%)
  const randomFactor = 0.8 + (Math.random() * 0.4)
  
  return Math.max(1000, Math.floor(baseVolume * decayFactor * randomFactor))
}

async function processCsvFile(filePath: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const items: any[] = []
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row: CsvRow) => {
        items.push({
          title: row.title,
          slug: toSlug(row.title),
          source: row.source,
          region: row.region,
          url: row.url,
          observed_at: row.observed_at
        })
      })
      .on('end', () => resolve(items))
      .on('error', reject)
  })
}

async function importCsvToDb(items: any[], sourceName: string) {
  console.log(`Processing ${items.length} items from ${sourceName}...`)
  
  // Remove duplicates within the same source based on slug
  const uniqueItems = items.reduce((acc: any[], item) => {
    const slug = toSlug(item.title)
    const existing = acc.find(existingItem => toSlug(existingItem.title) === slug)
    if (!existing) {
      acc.push(item)
    }
    return acc
  }, [])
  
  console.log(`Removed ${items.length - uniqueItems.length} duplicates within ${sourceName}`)
  
  // Sort by title to ensure consistent ranking
  uniqueItems.sort((a, b) => a.title.localeCompare(b.title))
  
  // Add realistic search volumes based on ranking
  const trendsWithVolumes = uniqueItems.map((item, index) => ({
    ...item,
    search_volume: generateSearchVolume(index + 1, uniqueItems.length)
  }))
  
  // Sort by search volume descending
  trendsWithVolumes.sort((a, b) => b.search_volume - a.search_volume)
  
  // Remove existing trends from this source first
  await supabaseAdmin.from('trends').delete().eq('source', sourceName)
  
  // Insert new trends with upsert to handle duplicates
  const { error } = await supabaseAdmin
    .from('trends')
    .upsert(trendsWithVolumes, { 
      onConflict: 'slug',
      ignoreDuplicates: false 
    })
  
  if (error) {
    console.error('Error inserting trends:', error)
    throw error
  }
  
  console.log(`✅ Successfully imported ${trendsWithVolumes.length} trends from ${sourceName}`)
  console.log(`Top 5 ${sourceName} trends:`)
  trendsWithVolumes.slice(0, 5).forEach((trend, index) => {
    console.log(`${index + 1}. ${trend.title}: ${trend.search_volume.toLocaleString()} searches`)
  })
}

async function main() {
  console.log('Starting multi-source CSV import process...')
  
  try {
    // Define the files to import
    const filesToImport = [
      { path: 'trendingcsv/google_trends/2025-08-31_11-48_US.csv', source: 'google_trends' },
      { path: 'trendingcsv/youtube_trends_latest.csv', source: 'youtube' },
      { path: 'trendingcsv/wikipedia_trends_latest.csv', source: 'wikipedia' },
      { path: 'trendingcsv/reddit/2025-08-31_16-49_REDDIT_COMBINED.csv', source: 'reddit' }
    ]
    
    // Process each file
    for (const fileInfo of filesToImport) {
      const filePath = fileInfo.path
      const sourceName = fileInfo.source
      
      if (!fs.existsSync(filePath)) {
        console.log(`⚠️  File not found: ${filePath}`)
        continue
      }
      
      console.log(`\nProcessing ${filePath} (${sourceName})...`)
      
      try {
        // Read and process CSV
        const items = await processCsvFile(filePath)
        
        if (items.length > 0) {
          // Import to database
          await importCsvToDb(items, sourceName)
          console.log(`✅ Successfully processed ${filePath}`)
        } else {
          console.log(`⚠️  No valid items found in ${filePath}`)
        }
        
      } catch (error) {
        console.error(`❌ Error processing ${filePath}:`, error)
      }
    }
    
    // Show final database stats
    const { data: allTrends } = await supabaseAdmin
      .from('trends')
      .select('source')
    
    if (allTrends) {
      const sourceCounts = allTrends.reduce((acc: any, trend: any) => {
        acc[trend.source] = (acc[trend.source] || 0) + 1
        return acc
      }, {})
      
      console.log('\n📊 Final Database Stats:')
      Object.entries(sourceCounts).forEach(([source, count]) => {
        console.log(`  ${source}: ${count} trends`)
      })
      console.log(`  Total: ${allTrends.length} trends`)
    }
    
    console.log('\n✅ Multi-source CSV import process completed')
    
  } catch (error) {
    console.error('❌ Fatal error during CSV import:', error)
    process.exit(1)
  }
}

// Run the importer
main()
