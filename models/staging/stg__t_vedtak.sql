-- stg__t_vedtak
-- henter nyeste rad fra t_vedtak

select *
from (
    select 
        *,
        row_number() over (
            partition by vedtak_id
            order by _olake_timestamp desc
        ) as row_num
    from {{ source('lakekeeper', 't_vedtak') }}
    -- where _op_type != 'd' -- vi har ikke sletting enda
)
where row_num = 1
