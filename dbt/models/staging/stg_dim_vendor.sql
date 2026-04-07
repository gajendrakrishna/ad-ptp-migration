{{ config(materialized='table') }}

with source_vendors as (
    select * from {{ source('master', 'vendors') }}
),

existing_dim_vendor as (
    select * from {{ this }}
),

expired_rows as (
    select
        dw.vendor_sk,
        dw.vendor_bk,
        dw.vendor_name,
        dw.contact_name,
        dw.city,
        dw.country,
        dw.tax_id,
        dw.payment_terms,
        dw.vendor_status,
        dw.effective_start,
        dateadd(day, -1, cast(current_date as date)) as effective_end,
        0 as is_current,
        dw.dw_insert_date,
        cast(current_timestamp() as timestamp) as dw_update_date,
        dw.source_row_hash,
        {{ invocation_id }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from existing_dim_vendor as dw
    inner join source_vendors as src
        on dw.vendor_bk = src.vendor_code
        and dw.is_current = 1
    where dw.source_row_hash <> hashbytes(
        'SHA2_256',
        concat_ws(
            '|',
            src.vendor_name,
            coalesce(src.contact_name, ''),
            coalesce(src.city, ''),
            src.payment_terms,
            src.vendor_status
        )
    )
),

new_rows as (
    select
        null as vendor_sk,
        src.vendor_code as vendor_bk,
        src.vendor_name,
        coalesce(src.contact_name, 'Unknown') as contact_name,
        coalesce(src.city, 'Unknown') as city,
        coalesce(src.country, 'Unknown') as country,
        coalesce(src.tax_id, 'Unknown') as tax_id,
        src.payment_terms,
        src.vendor_status,
        cast(current_date as date) as effective_start,
        cast('9999-12-31' as date) as effective_end,
        1 as is_current,
        cast(current_timestamp() as timestamp) as dw_insert_date,
        cast(current_timestamp() as timestamp) as dw_update_date,
        hashbytes(
            'SHA2_256',
            concat_ws(
                '|',
                src.vendor_name,
                coalesce(src.contact_name, ''),
                coalesce(src.city, ''),
                src.payment_terms,
                src.vendor_status
            )
        ) as source_row_hash,
        {{ invocation_id }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from source_vendors as src
    where not exists (
        select 1
        from existing_dim_vendor as dw
        where dw.vendor_bk = src.vendor_code
            and dw.is_current = 1
    )
),

final as (
    select * from expired_rows
    union all
    select * from new_rows
)

select * from final
