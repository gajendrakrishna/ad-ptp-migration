{{ config(materialized='ephemeral') }}

with source_items as (

    select * from {{ source('master', 'items') }}

),

existing_current as (

    select product_bk
    from {{ source('dbo', 'dim_product') }}
    where iscurrent = 1

),

new_versions as (

    select
        src.itemcode as product_bk,
        src.itemname as productname,
        coalesce(src.description, '') as description,
        src.unitofmeasure,
        src.itemcategory as category,
        src.accountingtype,
        coalesce(src.glaccount, '00000') as glaccount,
        cast(current_date as date) as effectivestart,
        cast('9999-12-31' as date) as effectiveend,
        cast(1 as boolean) as iscurrent,
        current_timestamp() as dw_insertdate,
        {{ invocation_id() }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from source_items as src
    where not exists (
        select 1
        from existing_current as dw
        where dw.product_bk = src.itemcode
    )

),

final as (

    select
        product_bk,
        productname,
        description,
        unitofmeasure,
        category,
        accountingtype,
        glaccount,
        effectivestart,
        effectiveend,
        iscurrent,
        dw_insertdate,
        _dbt_run_id,
        _loaded_at
    from new_versions

)

select * from final
