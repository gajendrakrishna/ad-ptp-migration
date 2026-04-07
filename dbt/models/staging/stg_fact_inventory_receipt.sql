{{ config(
    materialized='incremental',
    unique_key=['gr_bk', 'product_sk'],
    incremental_strategy='merge'
) }}

with source_gr_lines as (
    select * from {{ source('purchasing', 'GoodsReceiptLines') }}
),

source_gr as (
    select * from {{ source('purchasing', 'GoodsReceipts') }}
),

source_po_lines as (
    select * from {{ source('purchasing', 'PurchaseOrderLines') }}
),

source_po as (
    select * from {{ source('purchasing', 'PurchaseOrders') }}
),

source_vendors as (
    select * from {{ source('master', 'Vendors') }}
),

source_items as (
    select * from {{ source('master', 'Items') }}
),

source_employees as (
    select * from {{ source('master', 'Employees') }}
),

dim_date as (
    select * from {{ source('dbo', 'dim_date') }}
),

dim_vendor as (
    select * from {{ source('dbo', 'Dim_Vendor') }}
),

dim_product as (
    select * from {{ source('dbo', 'Dim_Product') }}
),

dim_employee as (
    select * from {{ source('dbo', 'Dim_Employee') }}
),

dim_location as (
    select * from {{ source('dbo', 'dim_location') }}
),

-- Running cumulative received quantity per PO line,
-- ordered by receipt date and GR primary key (posted receipts only)
gr_running_total as (
    select
        grl.grlineid,
        grl.grid,
        grl.polineid,
        grl.quantityreceived,
        sum(grl.quantityreceived) over (
            partition by grl.polineid
            order by
                gr_inner.receiptdate asc,
                gr_inner.grid asc,
                grl.grlineid asc
            rows between unbounded preceding and current row
        ) as cumulativeqtyreceived
    from source_gr_lines as grl
    inner join source_gr as gr_inner
        on gr_inner.grid = grl.grid
    where gr_inner.grstatus = 'Posted'
),

-- Resolve item-to-location mapping rule
item_location_bk as (
    select
        itm.itemid,
        itm.itemcode,
        itm.itemcategory,
        case
            when itm.itemcategory = 'Office'
                then 'WH-SUPP-03'
            when itm.itemcategory = 'Hardware'
                and itm.itemcode like '%SWITCH%'
                then 'WH-NET-04'
            when itm.itemcategory = 'Hardware'
                then 'WH-IT-02'
            else 'WH-CORP-01'
        end as location_bk
    from source_items as itm
),

-- Join source transactional tables
receipts_joined as (
    select
        run.grlineid,
        run.quantityreceived,
        run.cumulativeqtyreceived,
        gr.grnumber,
        gr.grstatus,
        gr.receiptdate,
        gr.receivedbyid,
        pol.polineid,
        pol.quantity as quantity_ordered,
        pol.itemid,
        po.poid,
        po.ponumber,
        po.podate,
        po.expecteddelivery,
        po.vendorid
    from gr_running_total as run
    inner join source_gr_lines as grl
        on grl.grlineid = run.grlineid
    inner join source_gr as gr
        on gr.grid = run.grid
    inner join source_po_lines as pol
        on pol.polineid = run.polineid
    inner join source_po as po
        on po.poid = pol.poid
    where gr.grstatus = 'Posted'
),

-- Attach source vendor, item, and employee for dimension lookups
receipts_with_dims as (
    select
        rj.*,
        src_vnd.vendorcode,
        src_itm.itemcode,
        src_itm.itemcategory,
        ilbk.location_bk,
        src_emp.employeecode
    from receipts_joined as rj
    inner join source_vendors as src_vnd
        on src_vnd.vendorid = rj.vendorid
    inner join source_items as src_itm
        on src_itm.itemid = rj.itemid
    inner join item_location_bk as ilbk
        on ilbk.itemid = rj.itemid
    inner join source_employees as src_emp
        on src_emp.employeeid = rj.receivedbyid

    {% if is_incremental() %}
    where rj.receiptdate > (
        select max(dw_load_datetime) from {{ this }}
    )
    {% endif %}
),

-- Produce all target columns with dimension lookups
final as (
    select
        rwd.grnumber as gr_bk,
        rwd.ponumber as po_bk,
        coalesce(d_rcv.date_sk, 0) as receiptdate_sk,
        coalesce(d_exp.date_sk, 0) as expecteddelivery_sk,
        coalesce(vnd.vendor_sk, -1) as vendor_sk,
        coalesce(prod.product_sk, -1) as product_sk,
        coalesce(loc.location_sk, -1) as location_sk,
        coalesce(emp.employee_sk, -1) as receivedby_employee_sk,
        rwd.grstatus as gr_status,
        rwd.quantity_ordered as quantity_ordered,
        rwd.quantityreceived as quantity_this_receipt,
        rwd.cumulativeqtyreceived as cumulative_qty_received,
        cast(
            rwd.quantity_ordered - rwd.cumulativeqtyreceived
            as decimal(10, 3)
        ) as quantity_outstanding,
        cast(
            (rwd.cumulativeqtyreceived / rwd.quantity_ordered) * 100
            as decimal(5, 2)
        ) as receipt_completeness_pct,
        datediff(day, rwd.podate, rwd.receiptdate) as lead_time_days,
        case
            when rwd.expecteddelivery is not null
                then datediff(day, rwd.expecteddelivery, rwd.receiptdate)
            else null
        end as delivery_variance_days,
        current_timestamp() as dw_load_datetime,
        {{ invocation_id() }} as _dbt_run_id
    from receipts_with_dims as rwd
    left join dim_date as d_rcv
        on d_rcv.date_sk = cast(
            format(rwd.receiptdate, 'yyyyMMdd') as int
        )
    left join dim_date as d_exp
        on d_exp.date_sk = cast(
            format(rwd.expecteddelivery, 'yyyyMMdd') as int
        )
    left join dim_vendor as vnd
        on vnd.vendor_bk = rwd.vendorcode
        and vnd.iscurrent = 1
    left join dim_product as prod
        on prod.product_bk = rwd.itemcode
        and prod.iscurrent = 1
    left join dim_employee as emp
        on emp.employee_bk = rwd.employeecode
        and emp.iscurrent = 1
    left join dim_location as loc
        on loc.location_bk = rwd.location_bk
        and loc.iscurrent = 1
)

select * from final
