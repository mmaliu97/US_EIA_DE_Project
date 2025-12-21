{{ config(
    materialized='table'
) }}

with base as (
    select
        id,
        date_time::date as day,
        fueltype,
        amt_energy
    from {{ ref('stg_energy_data') }}
),

-- 1) Aggregate total energy per day + fueltype
daily_totals as (
    select
        day,
        fueltype,
        sum(amt_energy) as value
    from base
    group by 1, 2
),

-- 2) Compute daily total (sum across all fueltypes)
daily_totals_with_day_sum as (
    select
        dt.*,
        sum(dt.value) over (partition by day) as daily_total
    from daily_totals dt
),

-- 3) Compute daily share percentage
daily_share_calc as (
    select
        day,
        fueltype,
        value,
        daily_total,
        (value / nullif(daily_total, 0) * 100.0) as share_pct
    from daily_totals_with_day_sum
)

-- 4) Final select
select
    day::timestamp as date_time,
    fueltype,
    value as fueltype_daily_value,
    daily_total,
    share_pct
from daily_share_calc
order by date_time, share_pct desc
