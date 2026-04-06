create view rpt.vw_dlvy_variance_days as
SELECT
    fir.GR_BK                                               AS [GR Number],
    d_rcv.FullDate                                          AS [Receipt Date],
    d_exp.FullDate                                          AS [Expected Delivery],
    fir.Quantity_Ordered                                    AS [Total Ordered],
    fir.Quantity_This_Receipt                               AS [This Shipment Qty],
    fir.Cumulative_Qty_Received                             AS [Cumulative Received],
    fir.Quantity_Outstanding                                AS [Still Outstanding],
    fir.Receipt_Completeness_Pct                            AS [Completeness %],
    fir.Lead_Time_Days                                      AS [Lead Time Days],
    fir.Delivery_Variance_Days                              AS [Delivery Variance Days (+late/-early)],
    CASE
        WHEN fir.Delivery_Variance_Days > 0 THEN CONCAT('+', fir.Delivery_Variance_Days, ' days LATE')
        WHEN fir.Delivery_Variance_Days < 0 THEN CONCAT(ABS(fir.Delivery_Variance_Days), ' days EARLY')
        ELSE 'On Time'
    END                                                     AS [Delivery Status],
    loc.LocationName                                        AS [Receiving Location]
FROM        dbo.Fact_Inventory_Receipt  fir
INNER JOIN  dbo.Dim_Date    d_rcv   ON d_rcv.Date_SK     = fir.ReceiptDate_SK
LEFT  JOIN  dbo.Dim_Date    d_exp   ON d_exp.Date_SK     = fir.ExpectedDelivery_SK
LEFT  JOIN  dbo.Dim_Location loc    ON loc.Location_SK   = fir.Location_SK
WHERE       fir.PO_BK = 'PO-2026-0005'
GO
create view rpt.vw_one_req_2_POS as
SELECT
    fr.Requisition_BK,
    fr.Quantity_Requested,
    fr.Estimated_Line_Total                                 AS [Req Estimated Total $],
    fpo.PO_BK,
    v.VendorName                                            AS [Vendor],
    p.ProductName                                           AS [Product],
    fpo.Quantity_Ordered,
    fpo.PO_Line_Total                                       AS [PO Committed $],
    fpo.Price_Savings_vs_Estimate                           AS [Savings vs Estimate $],
    fpo.Approval_Duration_Days                              AS [Req-to-PO Days]
FROM        dbo.Fact_Requisition    fr
INNER JOIN  dbo.Fact_PurchaseOrder  fpo
        ON  fpo.Requisition_BK  = fr.Requisition_BK
       AND  fpo.Product_SK      = fr.Product_SK
INNER JOIN  dbo.Dim_Vendor          v   ON v.Vendor_SK   = fpo.Vendor_SK
INNER JOIN  dbo.Dim_Product         p   ON p.Product_SK  = fpo.Product_SK
WHERE       fr.Requisition_BK   = 'REQ-2026-0006'
GO
create view rpt.vw_price_variance as
SELECT
    ai.Invoice_BK                                           AS [Internal Invoice Ref],
    ai.Vendor_Invoice_Number                                AS [Vendor Invoice No],
    v.VendorName                                            AS [Vendor],
    p.ProductName                                           AS [Product],
    ai.PO_BK                                               AS [Source PO],
    ai.Quantity_Billed                                      AS [Qty Billed],
    ai.PO_Unit_Price                                        AS [PO Contracted Price $],
    ai.Invoiced_Unit_Price                                  AS [Vendor Invoiced Price $],
    ai.Invoiced_Unit_Price - ai.PO_Unit_Price               AS [Per-Unit Overcharge $],
    ai.PO_Line_Amount                                       AS [Expected Billing (PO x Qty) $],
    ai.Invoice_Line_Total                                   AS [Actual Billed Amount $],
    -- Price_Variance = (InvoicePrice - POPrice) x QtyBilled -- the key formula
    ai.Price_Variance                                       AS [Price Variance $ (formula)],
    FORMAT(ai.Price_Variance_Pct, 'N2') + '%'              AS [Variance %],
    ai.Invoice_Status                                       AS [Invoice Status],
    ai.Match_Status                                         AS [Match Result],
    ai.Is_Disputed                                          AS [Is Disputed Flag],
    ai.Is_Paid                                              AS [Is Paid Flag]
FROM        dbo.Fact_AP_Invoice ai
INNER JOIN  dbo.Dim_Vendor  v   ON v.Vendor_SK  = ai.Vendor_SK
INNER JOIN  dbo.Dim_Product p   ON p.Product_SK = ai.Product_SK
WHERE       ai.Is_Disputed  = 1;
GO
create view rpt.vw_proc_perf_summary as
SELECT
    v.VendorName                                            AS [Vendor],
    COUNT(DISTINCT fpo.PO_BK)                               AS [Total POs],
    COUNT(fpo.Fact_PO_SK)                                   AS [Total PO Lines],
    SUM(fpo.PO_Line_Total)                                  AS [Total Committed $],
    SUM(fpo.Price_Savings_vs_Estimate)                      AS [Total Negotiated Savings $],
    -- Receipt performance
    COUNT(DISTINCT fir.GR_BK)                               AS [Total GR Events],
    AVG(CAST(fir.Lead_Time_Days AS FLOAT))                  AS [Avg Lead Time Days],
    AVG(CAST(fir.Delivery_Variance_Days AS FLOAT))          AS [Avg Delivery Variance Days],
    SUM(CASE WHEN fir.Delivery_Variance_Days > 0 THEN 1 ELSE 0 END) AS [Late Deliveries],
    -- Invoice performance
    COUNT(DISTINCT fai.Invoice_BK)                          AS [Total Invoices],
    SUM(fai.Price_Variance)                                 AS [Total Price Variance $],
    SUM(CASE WHEN fai.Is_Disputed = 1 THEN 1 ELSE 0 END)   AS [Disputed Invoices],
    SUM(CASE WHEN fai.Is_Paid     = 1 THEN 1 ELSE 0 END)   AS [Paid Invoices]
FROM        dbo.Fact_PurchaseOrder          fpo
INNER JOIN  dbo.Dim_Vendor                  v
        ON  v.Vendor_SK     = fpo.Vendor_SK
LEFT  JOIN  dbo.Fact_Inventory_Receipt      fir
        ON  fir.PO_BK       = fpo.PO_BK
       AND  fir.Product_SK  = fpo.Product_SK
LEFT  JOIN  dbo.Fact_AP_Invoice             fai
        ON  fai.PO_BK       = fpo.PO_BK
       AND  fai.Product_SK  = fpo.Product_SK
GROUP BY    v.VendorName
GO
CREATE   VIEW rpt.vw_PTP_Global_Lifecycle AS

WITH
-- Aggregate multiple receipt events back to one row per PO-line
AggReceipts AS (
    SELECT
        PO_BK,
        Product_SK,
        SUM(Quantity_This_Receipt)          AS Total_Qty_Received,
        SUM(Quantity_Outstanding)           AS Final_Qty_Outstanding,
        MIN(Receipt_Completeness_Pct)       AS First_Completeness_Pct,
        MAX(Receipt_Completeness_Pct)       AS Final_Completeness_Pct,
        COUNT(DISTINCT GR_BK)               AS Shipment_Count,
        MIN(ReceiptDate_SK)                 AS First_Receipt_Date_SK,
        MAX(ReceiptDate_SK)                 AS Last_Receipt_Date_SK,
        AVG(CAST(Lead_Time_Days AS FLOAT))  AS Avg_Lead_Time_Days,
        MIN(Lead_Time_Days)                 AS Min_Lead_Time_Days,
        MAX(Lead_Time_Days)                 AS Max_Lead_Time_Days,
        MAX(Delivery_Variance_Days)         AS Max_Delivery_Variance_Days,
        MIN(Delivery_Variance_Days)         AS Min_Delivery_Variance_Days,
        MAX(Location_SK)                    AS Primary_Location_SK
    FROM dbo.Fact_Inventory_Receipt
    GROUP BY PO_BK, Product_SK
)
SELECT
    -- CONFORMED DIMENSIONS
    v.VendorName                                            AS [Vendor],
    v.VendorStatus                                          AS [Vendor Status],
    v.PaymentTerms                                          AS [Payment Terms],
    p.ProductName                                           AS [Product],
    p.Category                                              AS [Product Category],
    p.AccountingType                                        AS [Accounting Type],
    p.GLAccount                                             AS [GL Account],

    -- 1. REQUISITION STAGE
    r.Requisition_BK                                        AS [Req Number],
    d_req.FullDate                                          AS [Req Date],
    d_req.FiscalYear                                        AS [Req Fiscal Year],
    d_req.CalendarQuarterName                               AS [Req Calendar Quarter],
    req_emp.FullName                                        AS [Requested By],
    r.Quantity_Requested                                    AS [Req Qty],
    r.Estimated_Unit_Price                                  AS [Est Unit Price $],
    r.Estimated_Line_Total                                  AS [Est Line Total $],
    r.Requisition_Status                                    AS [Req Status],
    r.Is_Approved                                           AS [Req Approved?],
    r.Is_Rejected                                           AS [Req Rejected?],
    r.Approval_Duration_Days                                AS [Req Approval Days],

    -- 2. PURCHASE ORDER STAGE
    po.PO_BK                                                AS [PO Number],
    po.PO_Line_Number                                       AS [PO Line #],
    d_po.FullDate                                           AS [PO Date],
    d_po.FiscalYear                                         AS [PO Fiscal Year],
    d_exp_po.FullDate                                       AS [PO Expected Delivery],
    buyer_emp.FullName                                      AS [Buyer],
    po.PO_Status                                            AS [PO Status],
    po.Quantity_Ordered                                     AS [PO Qty Ordered],
    po.Agreed_Unit_Price                                    AS [Agreed Unit Price $],
    po.PO_Line_Total                                        AS [PO Line Total $],
    po.Estimated_Unit_Price                                 AS [Req Est Unit Price $],
    po.Price_Savings_vs_Estimate                            AS [Price Savings vs Estimate $],
    po.Approval_Duration_Days                               AS [Days Req to PO],

    -- 3. INVENTORY RECEIPT STAGE
    d_first_rcv.FullDate                                    AS [First Receipt Date],
    d_last_rcv.FullDate                                     AS [Last Receipt Date],
    rcv.Shipment_Count                                      AS [No. of Shipments],
    rcv.Total_Qty_Received                                  AS [Total Qty Received],
    rcv.Final_Completeness_Pct                              AS [Receipt Completeness %],
    CAST(rcv.Avg_Lead_Time_Days AS DECIMAL(5,1))            AS [Avg Lead Time Days],
    rcv.Max_Delivery_Variance_Days                          AS [Worst Delivery Variance Days],
    rcv.Min_Delivery_Variance_Days                          AS [Best Delivery Variance Days],
    CASE
        WHEN rcv.Max_Delivery_Variance_Days > 0  THEN 'Late'
        WHEN rcv.Max_Delivery_Variance_Days < 0  THEN 'Early'
        WHEN rcv.Max_Delivery_Variance_Days = 0  THEN 'On Time'
        ELSE 'No Data'
    END                                                     AS [Delivery Performance],
    recv_loc.LocationName                                   AS [Receiving Location],

    -- 4. AP INVOICE STAGE
    ai.Invoice_BK                                           AS [Internal Invoice Ref],
    ai.Vendor_Invoice_Number                                AS [Vendor Invoice No],
    d_inv.FullDate                                          AS [Invoice Date],
    d_due.FullDate                                          AS [Invoice Due Date],
    ai.Invoice_Status                                       AS [Invoice Status],
    ai.Match_Status                                         AS [Match Status],
    CASE ai.Is_Match_Passed WHEN 1 THEN 'PASS' ELSE 'FAIL' END AS [3-Way Match],
    ai.Is_Disputed                                          AS [Is Disputed?],
    ai.Is_Paid                                              AS [Is Paid?],
    ai.Quantity_Billed                                      AS [Qty Billed],
    ai.Invoiced_Unit_Price                                  AS [Invoiced Unit Price $],
    ai.PO_Unit_Price                                        AS [PO Unit Price $],
    ai.Invoice_Line_Total                                   AS [Invoice Line Total $],
    ai.PO_Line_Amount                                       AS [PO Committed Amount $],
    ai.Price_Variance                                       AS [Price Variance $ (Inv-PO)xQty],
    FORMAT(ai.Price_Variance_Pct, 'N2')+ '%'               AS [Price Variance %],
    ai.Days_Until_Due                                       AS [Days Until Due],
    ai.Days_Invoice_To_Receive                              AS [Invoice Processing Lag Days],

    -- LIFECYCLE SUMMARY METRICS
    DATEDIFF(DAY, d_req.FullDate,     d_po.FullDate)        AS [Days: Req to PO],
    DATEDIFF(DAY, d_po.FullDate,      d_first_rcv.FullDate) AS [Days: PO to First Receipt],
    DATEDIFF(DAY, d_first_rcv.FullDate, d_inv.FullDate)     AS [Days: Receipt to Invoice],
    DATEDIFF(DAY, d_req.FullDate,     d_inv.FullDate)        AS [Total Cycle Days (Req to Invoice)]

FROM        dbo.Fact_PurchaseOrder              po

LEFT  JOIN  dbo.Fact_Requisition                r
        ON  r.Requisition_BK                    = po.Requisition_BK
       AND  r.Product_SK                        = po.Product_SK

LEFT  JOIN  AggReceipts                         rcv
        ON  rcv.PO_BK                           = po.PO_BK
       AND  rcv.Product_SK                      = po.Product_SK

LEFT  JOIN  dbo.Fact_AP_Invoice                 ai
        ON  ai.PO_BK                            = po.PO_BK
       AND  ai.Product_SK                       = po.Product_SK

INNER JOIN  dbo.Dim_Vendor                      v
        ON  v.Vendor_SK                         = po.Vendor_SK
INNER JOIN  dbo.Dim_Product                     p
        ON  p.Product_SK                        = po.Product_SK

LEFT  JOIN  dbo.Dim_Employee                    req_emp
        ON  req_emp.Employee_SK                 = r.Requestor_Employee_SK
LEFT  JOIN  dbo.Dim_Employee                    buyer_emp
        ON  buyer_emp.Employee_SK               = po.Buyer_Employee_SK

LEFT  JOIN  dbo.Dim_Location                    recv_loc
        ON  recv_loc.Location_SK                = rcv.Primary_Location_SK

LEFT  JOIN  dbo.Dim_Date                        d_req
        ON  d_req.Date_SK                       = r.RequisitionDate_SK

LEFT  JOIN  dbo.Dim_Date                        d_po
        ON  d_po.Date_SK                        = po.PODate_SK
LEFT  JOIN  dbo.Dim_Date                        d_exp_po
        ON  d_exp_po.Date_SK                    = po.ExpectedDelivery_SK

LEFT  JOIN  dbo.Dim_Date                        d_first_rcv
        ON  d_first_rcv.Date_SK                 = rcv.First_Receipt_Date_SK
LEFT  JOIN  dbo.Dim_Date                        d_last_rcv
        ON  d_last_rcv.Date_SK                  = rcv.Last_Receipt_Date_SK

LEFT  JOIN  dbo.Dim_Date                        d_inv
        ON  d_inv.Date_SK                       = ai.InvoiceDate_SK
LEFT  JOIN  dbo.Dim_Date                        d_due
        ON  d_due.Date_SK                       = ai.DueDate_SK;
GO
