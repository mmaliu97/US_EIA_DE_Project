select
    date_time,
    respondent,
    timezone,
    value_units,

    (date_time at time zone 'UTC') at time zone 'America/Chicago'
        as inserted_at_local,

    sum(amt_energy) filter (where type_name = 'Coal')             as coal,
    sum(amt_energy) filter (where type_name = 'Solar')           as solar,
    sum(amt_energy) filter (where type_name = 'Wind')            as wind,
    sum(amt_energy) filter (where type_name = 'Geothermal')      as geothermal,
    sum(amt_energy) filter (where type_name = 'Natural Gas')     as natural_gas,
    sum(amt_energy) filter (where type_name = 'Nuclear')         as nuclear,
    sum(amt_energy) filter (where type_name = 'Battery storage') as battery_storage,
    sum(amt_energy) filter (where type_name = 'Petroleum')       as petroleum,
    sum(amt_energy) filter (where type_name = 'Hydro')           as hydro,
    sum(amt_energy) filter (where type_name = 'Battery')         as battery

from {{ ref('stg_energy_data') }}
group by
    date_time,
    respondent,
    timezone,
    value_units
