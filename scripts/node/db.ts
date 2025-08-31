import { createClient } from '@supabase/supabase-js'

// Create Supabase client for server-side operations
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || 'https://placeholder.supabase.co'
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || 'placeholder-key'

export const supabaseAdmin = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
})

// Create Supabase client for client-side operations
export const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL || 'https://placeholder.supabase.co',
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || 'placeholder-key'
)

export interface Trend {
  id: string
  title: string
  slug: string
  search_volume: number
  source: string
  region: string
  url?: string
  observed_at: string
  created_at: string
  updated_at: string
}

export interface TrendWithDetails {
  slug: string
  name: string
  score: number
  regions: string[]
  first_seen: string
  last_seen: string
  sources: string[]
  aliases: string[]
}

/**
 * Get trends with filtering options
 */
export async function getTrends(options: {
  region?: string
  since_hours?: number
  q?: string
  min_score?: number
  sort?: 'score' | 'first_seen' | 'last_seen' | 'alpha'
  limit?: number
  offset?: number
} = {}): Promise<TrendWithDetails[]> {
  // Create Supabase client with service role key for server-side operations
  const supabaseClient = createClient(
    'https://rqjlhvjrtlgqzmtaetjf.supabase.co',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJxamxodmpydGxncXptdGFldGpmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NjYwMzc2NCwiZXhwIjoyMDcyMTc5NzY0fQ.5XMz5DvU5hCAtnDwn6v-0XSKwhaQJO-ppfAVXW9mLaA'
  )

  let query = supabaseClient
    .from('trends')
    .select('*')

  // Apply filters
  if (options.region) {
    query = query.eq('region', options.region)
  }

  if (options.since_hours) {
    const sinceDate = new Date(Date.now() - options.since_hours * 60 * 60 * 1000)
    query = query.gte('observed_at', sinceDate.toISOString())
  }

  if (options.min_score !== undefined) {
    query = query.gte('search_volume', options.min_score)
  }

  if (options.q) {
    query = query.ilike('title', `%${options.q}%`)
  }

  // Apply sorting
  switch (options.sort) {
    case 'first_seen':
      query = query.order('created_at', { ascending: false })
      break
    case 'last_seen':
      query = query.order('observed_at', { ascending: false })
      break
    case 'alpha':
      query = query.order('title', { ascending: true })
      break
    case 'score':
    default:
      query = query.order('search_volume', { ascending: false })
      break
  }

  // Apply pagination
  if (options.offset) {
    query = query.range(options.offset, options.offset + (options.limit || 100) - 1)
  } else if (options.limit) {
    query = query.limit(options.limit)
  }

  const { data: trends, error } = await query

  if (error) {
    console.error('Error fetching trends:', error)
    throw error
  }

  // Transform to the expected format
  return (trends || []).map(trend => ({
    slug: trend.slug,
    name: trend.title,
    score: trend.search_volume,
    regions: [trend.region],
    first_seen: trend.created_at,
    last_seen: trend.observed_at,
    sources: [trend.source],
    aliases: [trend.title]
  }))
}

/**
 * Get a single trend by slug
 */
export async function getTrendBySlug(slug: string): Promise<TrendWithDetails | null> {
  const supabaseClient = createClient(
    'https://rqjlhvjrtlgqzmtaetjf.supabase.co',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJxamxodmpydGxncXptdGFldGpmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NjYwMzc2NCwiZXhwIjoyMDcyMTc5NzY0fQ.5XMz5DvU5hCAtnDwn6v-0XSKwhaQJO-ppfAVXW9mLaA'
  )

  const { data: trend, error } = await supabaseClient
    .from('trends')
    .select('*')
    .eq('slug', slug)
    .single()

  if (error || !trend) {
    return null
  }

  return {
    slug: trend.slug,
    name: trend.title,
    score: trend.search_volume,
    regions: [trend.region],
    first_seen: trend.created_at,
    last_seen: trend.observed_at,
    sources: [trend.source],
    aliases: [trend.title]
  }
}

// Stub functions for future use
export async function getEntityMentions(entityId: string) {
  return []
}

export async function createProfile(userId: string, plan: string = 'free') {
  // TODO: Implement when auth is added
}

export async function updateProfilePlan(userId: string, plan: string) {
  // TODO: Implement when auth is added
}

export async function getUserProfile(userId: string) {
  // TODO: Implement when auth is added
  return { plan: 'free' }
}
