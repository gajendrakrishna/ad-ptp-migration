{{ config(
    materialized='incremental',
    unique_key=['po_bk', 'po_line_number'],
    incremental_strategy='merge'
) }}

with source_purchase_orders as (
    select * from {{ source('purchasing', 'purchaseorders') }}
),

source_purchase_order_lines as (
    select * from {{ source('purchasing', 'purchaseorderlines') }}
),

source_vendors as (
    select * from {{ source('master', 'vendors') }}
),

source_items as (
    select * from {{ source('master', 'items') }}
),

source_employees as (
    select * from {{ source('master', 'employees') }}
),

source_requisitions as (
    select * from {{ source('purchasing', 'requisitions') }}
),

source_requisition_lines as (
    select * from {{ source('purchasing', 'requisitionlines') }}
),

dim_date as (
    select * from {{ source('dbo', 'dim_date') }}
),

dim_vendor as (
    select * from {{ ref('stg_dim_vendor') }}
),

dim_product as (
    select * from {{ ref('stg_dim_product') }}
),

dim_employee as (
    select * from {{ ref('stg_dim_employee') }}
),

po_lines_joined as (
    select
        po.poid,
        po.ponumber,
        po.podate,
        po.expecteddelivery,
        po.postatus,
        po.vendorid,
        po.orderedbyid,
        po.approvedbyid,
        po.requisitionid,
        pol.linenumber,
        pol.itemid,
        pol.quantity,
        pol.agreedunitprice,
        pol.requisitionlineid
    from source_purchase_orders as po
    inner join source_purchase_order_lines as pol
        on pol.poid = po.poid

    {% if is_incremental() %}
    where po.podate > (
        select max(dw_load_datetime) from {{ this }}
    )
    {% endif %}
),

po_with_source_dims as (
    select
        plj.poid,
        plj.ponumber,
        plj.podate,
        plj.expecteddelivery,
        plj.postatus,
        plj.orderedbyid,
        plj.approvedbyid,
        plj.requisitionid,
        plj.linenumber,
        plj.quantity,
        plj.agreedunitprice,
        plj.requisitionlineid,
        src_vnd.vendorcode,
        src_itm.itemcode,
        src_buyer.employeecode as buyercode,
        src_approver.employeecode as approvercode
    from po_lines_joined as plj
    inner join source_vendors as src_vnd
        on src_vnd.vendorid = plj.vendorid
    inner join source_items as src_itm
        on src_itm.itemid = plj.itemid
    inner join source_employees as src_buyer
        on src_buyer.employeeid = plj.orderedbyid
    left join source_employees as src_approver
        on src_approver.employeeid = plj.approvedbyid
),

po_with_requisition as (
    select
        psd.*,
        req.requisitionnumber,
        req.requisitiondate,
        rl.estimatedunitprice
    from po_with_source_dims as psd
    left join source_requisitions as req
        on req.requisitionid = psd.requisitionid
    left join source_requisition_lines as rl
        on rl.requisitionlineid = psd.requisitionlineid
),

po_with_date_sks as (
    select
        pwr.*,
        coalesce(d_po.date_sk, 0) as podate_sk,
        coalesce(d_exp.date_sk, 0) as expecteddelivery_sk
    from po_with_requisition as pwr
    left join dim_date as d_po
        on d_po.date_sk = cast(
            format(pwr.podate, 'yyyyMMdd') as int
        )
    left join dim_date as d_exp
        on d_exp.date_sk = cast(
            format(pwr.expecteddelivery, 'yyyyMMdd') as int
        )
),

po_with_dim_sks as (
    select
        pwd.*,
        coalesce(vnd.vendor_sk, -1) as vendor_sk,
        coalesce(prod.product_sk, -1) as product_sk,
        coalesce(emp_buyer.employee_sk, -1) as buyer_employee_sk,
        coalesce(emp_approver.employee_sk, -1) as approver_employee_sk
    from po_with_date_sks as pwd
    left join dim_vendor as vnd
        on vnd.vendor_bk = pwd.vendorcode
        and vnd.iscurrent = 1
    left join dim_product as prod
        on prod.product_bk = pwd.itemcode
        and prod.iscurrent = 1
    left join dim_employee as emp_buyer
        on emp_buyer.employee_bk = pwd.buyercode
        and emp_buyer.iscurrent = 1
    left join dim_employee as emp_approver
        on emp_approver.employee_bk = pwd.approvercode
        and emp_approver.iscurrent = 1
),

final as (
    select
        ponumber as po_bk,
        linenumber as po_line_number,
        requisitionnumber as requisition_bk,
        podate_sk,
        expecteddelivery_sk,
        vendor_sk,
        product_sk,
        buyer_employee_sk,
        approver_employee_sk,
        postatus as po_status,
        quantity as quantity_ordered,
        agreedunitprice as agreed_unit_price,
        cast(
            quantity * agreedunitprice as decimal(18, 2)
        ) as po_line_total,
        estimatedunitprice as estimated_unit_price,
        case
            when estimatedunitprice is not null
                then cast(
                    (estimatedunitprice - agreedunitprice) * quantity
                    as decimal(18, 2)
                )
            else null
        end as price_savings_vs_estimate,
        case
            when requisitiondate is not null
                then datediff(day, requisitiondate, podate)
            else null
        end as approval_duration_days,
        current_timestamp() as dw_load_datetime,
        {{ invocation_id }} as _dbt_run_id
    from po_with_dim_sks
)

select * from final
