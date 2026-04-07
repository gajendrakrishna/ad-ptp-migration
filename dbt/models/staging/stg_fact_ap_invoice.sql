{{ config(
    materialized='incremental',
    unique_key=['invoice_bk', 'invoice_line_number'],
    incremental_strategy='merge'
) }}

with source_invoices as (
    select * from {{ source('finance', 'invoices') }}
),

source_invoice_lines as (
    select * from {{ source('finance', 'invoicelines') }}
),

source_po_lines as (
    select * from {{ source('purchasing', 'purchaseorderlines') }}
),

source_purchase_orders as (
    select * from {{ source('purchasing', 'purchaseorders') }}
),

source_vendors as (
    select * from {{ source('master', 'vendors') }}
),

source_items as (
    select * from {{ source('master', 'items') }}
),

dim_date as (
    select * from {{ source('dbo', 'dim_date') }}
),

dim_vendor as (
    select * from {{ source('dbo', 'dim_vendor') }}
),

dim_product as (
    select * from {{ source('dbo', 'dim_product') }}
),

invoice_lines_joined as (
    select
        inv.invoiceid,
        inv.internalinvoiceref,
        inv.invoicenumber,
        inv.vendorid,
        inv.invoicedate,
        inv.receiveddate,
        inv.duedate,
        inv.invoicestatus,
        inv.matchstatus,
        il.linenumber,
        il.polineid,
        il.itemid,
        il.quantitybilled,
        il.invoicedunitprice,
        pol.agreedunitprice,
        po.ponumber,
        src_vnd.vendorcode,
        src_itm.itemcode
    from source_invoices as inv
    inner join source_invoice_lines as il
        on il.invoiceid = inv.invoiceid
    inner join source_po_lines as pol
        on pol.polineid = il.polineid
    inner join source_purchase_orders as po
        on po.poid = pol.poid
    inner join source_vendors as src_vnd
        on src_vnd.vendorid = inv.vendorid
    inner join source_items as src_itm
        on src_itm.itemid = il.itemid

    {% if is_incremental() %}
    where inv.updatedat > (
        select max(dw_load_datetime) from {{ this }}
    )
    {% endif %}
),

with_dimension_keys as (
    select
        ilj.invoiceid,
        ilj.internalinvoiceref,
        ilj.invoicenumber,
        ilj.vendorid,
        ilj.invoicedate,
        ilj.receiveddate,
        ilj.duedate,
        ilj.invoicestatus,
        ilj.matchstatus,
        ilj.linenumber,
        ilj.polineid,
        ilj.itemid,
        ilj.quantitybilled,
        ilj.invoicedunitprice,
        ilj.agreedunitprice,
        ilj.ponumber,
        ilj.vendorcode,
        ilj.itemcode,
        coalesce(d_inv.date_sk, 0) as invoicedate_sk,
        coalesce(d_rcv.date_sk, 0) as receiveddate_sk,
        coalesce(d_due.date_sk, 0) as duedate_sk,
        coalesce(vnd.vendor_sk, -1) as vendor_sk,
        coalesce(prod.product_sk, -1) as product_sk
    from invoice_lines_joined as ilj
    left join dim_date as d_inv
        on d_inv.date_sk = cast(
            format(ilj.invoicedate, 'yyyyMMdd') as int
        )
    left join dim_date as d_rcv
        on d_rcv.date_sk = cast(
            format(ilj.receiveddate, 'yyyyMMdd') as int
        )
    left join dim_date as d_due
        on d_due.date_sk = cast(
            format(ilj.duedate, 'yyyyMMdd') as int
        )
    left join dim_vendor as vnd
        on vnd.vendor_bk = ilj.vendorcode
        and vnd.iscurrent = 1
    left join dim_product as prod
        on prod.product_bk = ilj.itemcode
        and prod.iscurrent = 1
),

final as (
    select
        internalinvoiceref as invoice_bk,
        invoicenumber as vendor_invoice_number,
        linenumber as invoice_line_number,
        ponumber as po_bk,
        invoicedate_sk,
        receiveddate_sk,
        duedate_sk,
        vendor_sk,
        product_sk,
        invoicestatus as invoice_status,
        matchstatus as match_status,
        case
            when matchstatus = 'Matched' then 1
            else 0
        end as is_match_passed,
        case
            when invoicestatus = 'Disputed' then 1
            else 0
        end as is_disputed,
        case
            when invoicestatus = 'Paid' then 1
            else 0
        end as is_paid,
        quantitybilled as quantity_billed,
        invoicedunitprice as invoiced_unit_price,
        agreedunitprice as po_unit_price,
        cast(
            quantitybilled * invoicedunitprice as decimal(18, 2)
        ) as invoice_line_total,
        cast(
            quantitybilled * agreedunitprice as decimal(18, 2)
        ) as po_line_amount,
        cast(
            (invoicedunitprice - agreedunitprice) * quantitybilled
            as decimal(18, 2)
        ) as price_variance,
        case
            when agreedunitprice <> 0
            then cast(
                ((invoicedunitprice - agreedunitprice) / agreedunitprice)
                * 100 as decimal(8, 4)
            )
            else null
        end as price_variance_pct,
        datediff(day, invoicedate, duedate) as days_until_due,
        datediff(
            day, invoicedate, receiveddate
        ) as days_invoice_to_receive,
        current_timestamp() as dw_load_datetime,
        {{ invocation_id() }} as _dbt_run_id
    from with_dimension_keys
)

select * from final
