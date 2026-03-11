-- Align plans table with current application model.
-- invoice_cadence and trial_period now live on prices / subscription line items,
-- and are no longer part of the Plan entity.

ALTER TABLE public.plans
    DROP COLUMN IF EXISTS invoice_cadence,
    DROP COLUMN IF EXISTS trial_period;