-- antall_duplikater_t_vedtak
-- teller antall duplikater på gyldige rader i t_vedtak og i snapshot av t_vedtak

select
    'iceberg snapshot' as kilde,
    count(ant_gyldige_versjoner) as antall_rader,
    ant_gyldige_versjoner
from
(select 
    vedtak_id,
    _op_type,
    count(*) ant_gyldige_versjoner
from {{ source('lakekeeper', 't_vedtak') }}
-- from copy_pen_q2_to_iceberg_kimtore_pen.t_vedtak
where dato_lopende_tom is not null
group by vedtak_id, _op_type
order by count(*) desc
)
group by ant_gyldige_versjoner

union all

select
    'deduplisert stg' as kilde,
    count(ant_gyldige_versjoner) as antall_rader,
    ant_gyldige_versjoner
from
(select 
    vedtak_id,
    _op_type,
    count(*) ant_gyldige_versjoner
from {{ ref ('stg__t_vedtak') }}
where dato_lopende_tom is not null
group by vedtak_id, _op_type
order by count(*) desc
)
group by ant_gyldige_versjoner
