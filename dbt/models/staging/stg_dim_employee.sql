{{ config(materialized='table') }}

with source_employees as (
    select * from {{ source('master', 'employees') }}
),

existing_dim_employee as (
    select * from {{ source('dbo', 'dim_employee') }}
),

changed_employees as (
    select
        dw.employee_sk,
        dw.employee_bk,
        dw.full_name,
        dw.email,
        dw.department,
        dw.job_title,
        dw.is_active,
        dw.effective_start,
        dw.effective_end,
        dw.is_current,
        dw.dw_insert_date,
        dw.source_row_hash
    from existing_dim_employee as dw
    inner join source_employees as src
        on dw.employee_bk = src.employee_code
        and dw.is_current = 1
    where dw.source_row_hash <> sha2(
        concat_ws(
            '|',
            src.full_name,
            src.email,
            src.department,
            src.job_title,
            cast(src.is_active as char(1))
        ),
        256
    )
),

expired_rows as (
    select
        dw.employee_sk,
        dw.employee_bk,
        dw.full_name,
        dw.email,
        dw.department,
        dw.job_title,
        dw.is_active,
        dw.effective_start,
        dateadd(day, -1, cast(current_date() as date)) as effective_end,
        0 as is_current,
        dw.dw_insert_date,
        dw.source_row_hash
    from changed_employees as dw
),

unchanged_rows as (
    select
        dw.employee_sk,
        dw.employee_bk,
        dw.full_name,
        dw.email,
        dw.department,
        dw.job_title,
        dw.is_active,
        dw.effective_start,
        dw.effective_end,
        dw.is_current,
        dw.dw_insert_date,
        dw.source_row_hash
    from existing_dim_employee as dw
    where not exists (
        select 1
        from changed_employees as c
        where c.employee_sk = dw.employee_sk
    )
),

new_rows as (
    select
        null as employee_sk,
        src.employee_code as employee_bk,
        src.full_name,
        src.email,
        src.department,
        src.job_title,
        src.is_active,
        cast(current_date() as date) as effective_start,
        cast('9999-12-31' as date) as effective_end,
        1 as is_current,
        cast(current_timestamp() as timestamp) as dw_insert_date,
        sha2(
            concat_ws(
                '|',
                src.full_name,
                src.email,
                src.department,
                src.job_title,
                cast(src.is_active as char(1))
            ),
            256
        ) as source_row_hash
    from source_employees as src
    where not exists (
        select 1
        from existing_dim_employee as dw
        where dw.employee_bk = src.employee_code
            and dw.is_current = 1
    )
),

final as (
    select
        employee_sk,
        employee_bk,
        full_name,
        email,
        department,
        job_title,
        is_active,
        effective_start,
        effective_end,
        is_current,
        dw_insert_date,
        source_row_hash,
        {{ invocation_id() }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from expired_rows
    union all
    select
        employee_sk,
        employee_bk,
        full_name,
        email,
        department,
        job_title,
        is_active,
        effective_start,
        effective_end,
        is_current,
        dw_insert_date,
        source_row_hash,
        {{ invocation_id() }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from unchanged_rows
    union all
    select
        employee_sk,
        employee_bk,
        full_name,
        email,
        department,
        job_title,
        is_active,
        effective_start,
        effective_end,
        is_current,
        dw_insert_date,
        source_row_hash,
        {{ invocation_id() }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from new_rows
)

select * from final
