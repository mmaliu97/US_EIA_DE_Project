{{ config(
    materialized='table'
) }}

with base as (
    select
        id,
        date_time,
        timezone,
        type_name,
        amt_energy
    from {{ ref('stg_energy_data') }}
),

-- 1) Aggregate total energy per month + type_name + timezone
monthly_totals as (
    select
        date_trunc('month', date_time) as month_start,
        timezone,
        type_name,
        sum(amt_energy) as value
    from base
    group by
        month_start,
        timezone,
        type_name
),

-- 2) Compute monthly total (sum across all type_names) per timezone
monthly_totals_with_month_sum as (
    select
        mt.*,
        sum(mt.value) over (
            partition by mt.month_start, mt.timezone
        ) as monthly_total
    from monthly_totals mt
),

-- 3) Compute share percentage
monthly_share_calc as (
    select
        month_start,
        timezone,
        type_name,
        value,
        monthly_total,
        (value / nullif(monthly_total, 0) * 100.0) as share_pct
    from monthly_totals_with_month_sum
)

-- 4) Final select
select
    month_start::date as month,
    timezone,
    type_name,
    value as fueltype_monthly_value,
    monthly_total,
    share_pct
from monthly_share_calc
order by
    month,
    timezone,
    share_pct desc
