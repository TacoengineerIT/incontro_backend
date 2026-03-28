-- ============================================================
-- Incontro Studio Hub — Supabase Schema
-- Run this once in the Supabase SQL editor before starting
-- ============================================================

CREATE TABLE IF NOT EXISTS public.users (
  id TEXT PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  password_salt TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  is_verified BOOLEAN DEFAULT FALSE,
  study_subjects TEXT[] DEFAULT '{}',
  learning_style TEXT,
  created_at FLOAT8 DEFAULT EXTRACT(EPOCH FROM NOW()),
  lat FLOAT8,
  lon FLOAT8,
  is_studying BOOLEAN DEFAULT FALSE,
  study_location_name TEXT,
  study_started_at FLOAT8,
  username TEXT UNIQUE,
  avatar_base64 TEXT,
  followers TEXT[] DEFAULT '{}',
  following TEXT[] DEFAULT '{}',
  has_active_story BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS public.stories (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  image_base64 TEXT NOT NULL,
  caption TEXT,
  created_at FLOAT8 NOT NULL
);

CREATE TABLE IF NOT EXISTS public.swipes (
  from_user_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  to_user_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  direction TEXT NOT NULL,
  created_at FLOAT8,
  PRIMARY KEY (from_user_id, to_user_id)
);

CREATE TABLE IF NOT EXISTS public.matches (
  id TEXT PRIMARY KEY,
  user_a_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  user_b_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  matched_at FLOAT8,
  UNIQUE(user_a_id, user_b_id)
);

CREATE TABLE IF NOT EXISTS public.messages (
  id TEXT PRIMARY KEY,
  match_id TEXT NOT NULL REFERENCES public.matches(id) ON DELETE CASCADE,
  from_user_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  text TEXT NOT NULL,
  created_at FLOAT8 NOT NULL
);

-- Disable RLS so the service role can read/write freely
ALTER TABLE public.users DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.stories DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.swipes DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.matches DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.messages DISABLE ROW LEVEL SECURITY;
