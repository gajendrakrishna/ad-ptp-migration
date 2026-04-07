{{ config(
    materialized='incremental',
    unique_key=['requisition_bk', 'requisitionline_bk'],
    incremental_strategy='merge'
) }}

with source_requisitions as (
    select * from {{ source('purchasing', 'requisitions') }}
),

source_requisition_lines as (
    select * from {{ source('purchasing', 'requisitionlines') }}
),

source_requestor_employees as (
    select * from {{ source('master', 'employees') }}
),

source_approver_employees as (
    select * from {{ source('master', 'employees') }}
),

source_items as (
    select * from {{ source('master', 'items') }}
),

source_vendors as (
    select * from {{ source('master', 'vendors') }}
),

dim_date as (
    select * from {{ source('dbo', 'dim_date') }}
),

dim_employee as (
    select * from {{ ref('stg_dim_employee') }}
),

dim_product as (
    select * from {{ ref('stg_dim_product') }}
),

dim_vendor as (
    select * from {{ ref('stg_dim_vendor') }}
),

joined as (
    select
        r.requisitionnumber,
        rl.requisitionlineid,
        r.requisitionstatus,
        r.requisitiondate,
        r.requiredbydate,
        r.updatedat,
        rl.quantity,
        rl.estimatedunitprice,
        rl.suggestedvendorid,
        src_req_emp.employeecode as requestor_employeecode,
        src_apr_emp.employeecode as approver_employeecode,
        src_itm.itemcode,
        src_svnd.vendorcode as suggestedvendorcode
    from source_requisitions as r
    inner join source_requisition_lines as rl
        on rl.requisitionid = r.requisitionid
    inner join source_requestor_employees as src_req_emp
        on src_req_emp.employeeid = r.requestedbyid
    inner join source_items as src_itm
        on src_itm.itemid = rl.itemid
    left join source_approver_employees as src_apr_emp
        on src_apr_emp.employeeid = r.approvedbyid
    left join source_vendors as src_svnd
        on src_svnd.vendorid = rl.suggestedvendorid

    {% if is_incremental() %}
    where r.updatedat > (
        select max(dw_load_datetime) from {{ this }}
    )
    {% endif %}
),

with_lookups as (
    select
        j.requisitionnumber,
        j.requisitionlineid,
        j.requisitionstatus,
        j.requisitiondate,
        j.requiredbydate,
        j.updatedat,
        j.quantity,
        j.estimatedunitprice,
        d_req.date_sk as requisitiondate_sk,
        d_rby.date_sk as requiredbydate_sk,
        emp_req.employee_sk as requestor_employee_sk,
        emp_apr.employee_sk as approver_employee_sk,
        prod.product_sk,
        svnd.vendor_sk as suggestedvendor_sk
    from joined as j
    left join dim_date as d_req
        on d_req.date_sk = cast(
            format(j.requisitiondate, 'yyyyMMdd') as int
        )
    left join dim_date as d_rby
        on d_rby.date_sk = cast(
            format(j.requiredbydate, 'yyyyMMdd') as int
        )
    left join dim_employee as emp_req
        on emp_req.employee_bk = j.requestor_employeecode
        and emp_req.iscurrent = 1
    left join dim_employee as emp_apr
        on emp_apr.employee_bk = j.approver_employeecode
        and emp_apr.iscurrent = 1
    left join dim_product as prod
        on prod.product_bk = j.itemcode
        and prod.iscurrent = 1
    left join dim_vendor as svnd
        on svnd.vendor_bk = j.suggestedvendorcode
        and svnd.iscurrent = 1
),

final as (
    select
        requisitionnumber as requisition_bk,
        requisitionlineid as requisitionline_bk,
        coalesce(requisitiondate_sk, 0) as requisitiondate_sk,
        coalesce(requiredbydate_sk, 0) as requiredbydate_sk,
        coalesce(requestor_employee_sk, -1) as requestor_employee_sk,
        coalesce(approver_employee_sk, -1) as approver_employee_sk,
        coalesce(product_sk, -1) as product_sk,
        coalesce(suggestedvendor_sk, -1) as suggestedvendor_sk,
        requisitionstatus as requisition_status,
        case
            when requisitionstatus in ('Approved', 'Converted') then 1
            else 0
        end as is_approved,
        case
            when requisitionstatus = 'Rejected' then 1
            else 0
        end as is_rejected,
        case
            when requisitionstatus = 'Converted' then 1
            else 0
        end as is_converted_to_po,
        quantity as quantity_requested,
        estimatedunitprice as estimated_unit_price,
        cast(
            quantity * estimatedunitprice as decimal(18, 2)
        ) as estimated_line_total,
        case
            when requisitionstatus in ('Approved', 'Rejected', 'Converted')
            then datediff(
                day,
                requisitiondate,
                cast(updatedat as date)
            )
            else null
        end as approval_duration_days,
        current_timestamp() as dw_load_datetime,
        {{ invocation_id }} as _dbt_run_id
    from with_lookups
)

select * from final
