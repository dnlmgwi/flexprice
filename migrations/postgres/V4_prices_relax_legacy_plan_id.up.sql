-- Align prices table with current application model.
-- Runtime code now scopes prices via entity_type/entity_id, but some legacy
-- tooling still references plan_id, so keep the column and relax the constraint.

ALTER TABLE public.prices
    ALTER COLUMN plan_id DROP NOT NULL;