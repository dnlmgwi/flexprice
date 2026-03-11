-- Align subscriptions table with the current application model.
-- Runtime code uses billing_cadence/billing_period on subscriptions, while
-- the legacy invoice_cadence column is no longer populated on subscription
-- rows.

ALTER TABLE public.subscriptions
    ALTER COLUMN invoice_cadence DROP NOT NULL;