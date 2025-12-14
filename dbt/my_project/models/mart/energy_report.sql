{{ config(
    materialized='table',
    unique_key='id'
)}}

select 
    id,
    date_time,
    extract(month from date_time) as month,
    respondent,
    fueltype,
    timezone,
    amt_energy,
    value_units

from {{ ref('stg_energy_data') }}
