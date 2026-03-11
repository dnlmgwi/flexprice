-- Align tenants table with the current application model.
-- Self-hosted signup defaults the initial tenant name to "Flexprice", and the
-- onboarding UI lets users rename it later. The current app schema does not
-- require tenant names to be globally unique.

ALTER TABLE public.tenants
    DROP CONSTRAINT IF EXISTS tenants_name_key;