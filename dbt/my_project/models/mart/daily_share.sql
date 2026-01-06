{{ config(
    materialized='table'
) }}

with base as (
    select
        id,
        date_time::date as day,
        type_name,
        timezone,
        amt_energy
    from {{ ref('stg_energy_data') }}
),

daily_totals as (
    select
        day,
        type_name,
        timezone,
        sum(amt_energy) as value
    from base
    group by
        day,
        type_name,
        timezone
),

-- 2) Compute daily total (sum across all type_names)
daily_totals_with_day_sum as (
    select
        dt.*,
        sum(dt.value) over (
            partition by dt.day, dt.timezone
        ) as daily_total
    from daily_totals dt
)


-- 3) Compute daily share percentage
daily_share_calc as (
    select
        day,
        type_name,
        timezone,
        value,
        daily_total,
        (value / daily_total * 100.0) as share_pct
    from daily_totals_with_day_sum
)

-- 4) Final select
select
    day,
    type_name,
    timezone,
    value as fueltype_daily_value,
    daily_total,
    share_pct
from daily_share_calc
order by day, share_pct desc
