{{ config(
    materialized='table'
)}}

with source as (
    select *
    from {{ source('dev', 'raw_eia_fuel_data') }}
),

de_dup as (
    select
        *,
        row_number() over (
            partition by period, fueltype, respondent
            order by period
        ) as rn
    from source
)

select
    -- Create custom unique ID
    concat(
        to_char(period, 'DD_MM_YYYY'), '_',
        respondent, '_',
        fueltype, '_',
        replace(timezone_description, ' ', '')
    ) as id,

    period as date_time,
    respondent,
    respondent_name,
    fueltype,
    type_name,
    timezone,
    timezone_description,
    value as amt_energy,
    value_units,

    -- Convert UTC to Central Time
    (period AT TIME ZONE 'UTC') AT TIME ZONE 'America/Chicago'
        as inserted_at_local

from de_dup
where rn = 1
