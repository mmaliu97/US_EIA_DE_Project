{{ config(
    materialized='table'
) }}

with base as (
    select
        id,
        date_time,
        type_name,
        amt_energy
    from {{ ref('stg_energy_data') }}
),

-- 1) Aggregate total energy per month + type_name
monthly_totals as (
    select
        date_trunc('month', date_time) as month_start,
        type_name,
        sum(amt_energy) as value
    from base
    group by 1, 2
),

-- 2) Compute monthly total (sum across all type_names)
monthly_totals_with_month_sum as (
    select
        mt.*,
        sum(mt.value) over (partition by month_start) as monthly_total
    from monthly_totals mt
),

-- 3) Compute share percentage
monthly_share_calc as (
    select
        month_start,
        type_name,
        value,
        monthly_total,
        (value / nullif(monthly_total, 0) * 100.0) as share_pct
    from monthly_totals_with_month_sum
)

-- 4) Final select
select
    month_start::date as month,
    type_name,
    value as fueltype_monthly_value,
    monthly_total,
    share_pct
from monthly_share_calc
order by month, share_pct desc
