-- ────────────────────────────────────────────────────────────
-- etl.usp_Load_Dim_Employee
-- Tracks changes to: FullName, Email, Department, JobTitle, IsActive
-- ────────────────────────────────────────────────────────────
CREATE   PROCEDURE etl.usp_Load_Dim_Employee
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Today    DATE = CAST(GETDATE() AS DATE);
    DECLARE @Expired  INT  = 0,  @Inserted INT = 0;

    -- Phase 1: Expire changed rows
    UPDATE dw
    SET    dw.EffectiveEnd = DATEADD(DAY, -1, @Today),
           dw.IsCurrent    = 0
    FROM   dbo.Dim_Employee dw
    INNER JOIN PTP_System.Master.Employees src
           ON  dw.Employee_BK = src.EmployeeCode
          AND  dw.IsCurrent   = 1
    WHERE  dw.SourceRowHash <> HASHBYTES('SHA2_256',
                                    CONCAT_WS('|',
                                        src.FullName,
                                        src.Email,
                                        src.Department,
                                        src.JobTitle,
                                        CAST(src.IsActive AS CHAR(1))));

    SET @Expired = @@ROWCOUNT;

    -- Phase 2: Insert new and changed
    INSERT INTO dbo.Dim_Employee
        (Employee_BK, FullName, Email, Department, JobTitle, IsActive,
         EffectiveStart, EffectiveEnd, IsCurrent, DW_InsertDate, SourceRowHash)
    SELECT
        src.EmployeeCode,
        src.FullName,
        src.Email,
        src.Department,
        src.JobTitle,
        src.IsActive,
        @Today,
        '9999-12-31',
        1,
        SYSUTCDATETIME(),
        HASHBYTES('SHA2_256',
            CONCAT_WS('|',
                src.FullName,
                src.Email,
                src.Department,
                src.JobTitle,
                CAST(src.IsActive AS CHAR(1))))
    FROM PTP_System.Master.Employees src
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.Dim_Employee dw
        WHERE  dw.Employee_BK = src.EmployeeCode
          AND  dw.IsCurrent   = 1
    );

    SET @Inserted = @@ROWCOUNT;
    PRINT CONCAT('usp_Load_Dim_Employee: ', @Inserted, ' inserted, ', @Expired, ' expired.');
END;
GO
-- ────────────────────────────────────────────────────────────
-- etl.usp_Load_Dim_Product
-- Tracks changes to: ItemName, Description, UOM, Category, AccountingType, GLAccount
-- ────────────────────────────────────────────────────────────
CREATE   PROCEDURE etl.usp_Load_Dim_Product
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Today    DATE = CAST(GETDATE() AS DATE);
    DECLARE @Expired  INT  = 0,  @Inserted INT = 0;

    -- Phase 1: Expire changed rows
    UPDATE dw
    SET    dw.EffectiveEnd = DATEADD(DAY, -1, @Today),
           dw.IsCurrent    = 0
    FROM   dbo.Dim_Product dw
    INNER JOIN PTP_System.Master.Items src
           ON  dw.Product_BK = src.ItemCode
          AND  dw.IsCurrent  = 1
    WHERE  dw.SourceRowHash <> HASHBYTES('SHA2_256',
                                    CONCAT_WS('|',
                                        src.ItemName,
                                        ISNULL(src.Description,''),
                                        src.UnitOfMeasure,
                                        src.ItemCategory,
                                        src.AccountingType,
                                        ISNULL(src.GLAccount,'')));

    SET @Expired = @@ROWCOUNT;

    -- Phase 2: Insert new and changed versions
    INSERT INTO dbo.Dim_Product
        (Product_BK, ProductName, Description, UnitOfMeasure, Category,
         AccountingType, GLAccount,
         EffectiveStart, EffectiveEnd, IsCurrent, DW_InsertDate, SourceRowHash)
    SELECT
        src.ItemCode,
        src.ItemName,
        ISNULL(src.Description,  ''),
        src.UnitOfMeasure,
        src.ItemCategory,
        src.AccountingType,
        ISNULL(src.GLAccount, '00000'),
        @Today,
        '9999-12-31',
        1,
        SYSUTCDATETIME(),
        HASHBYTES('SHA2_256',
            CONCAT_WS('|',
                src.ItemName,
                ISNULL(src.Description,''),
                src.UnitOfMeasure,
                src.ItemCategory,
                src.AccountingType,
                ISNULL(src.GLAccount,'')))
    FROM PTP_System.Master.Items src
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.Dim_Product dw
        WHERE  dw.Product_BK = src.ItemCode
          AND  dw.IsCurrent  = 1
    );

    SET @Inserted = @@ROWCOUNT;
    PRINT CONCAT('usp_Load_Dim_Product: ', @Inserted, ' inserted, ', @Expired, ' expired.');
END;
GO
-- ============================================================
-- PART 3: ETL STORED PROCEDURES
-- ============================================================
-- Naming convention : etl.usp_Load_<Target>
-- Source            : PTP_System (cross-DB, same SQL Server instance)
-- Dimension load    : SCD Type 2 via two-phase UPDATE + INSERT
--                     (expire changed rows -> re-insert with new version)
-- Fact load         : Full truncate-reload
--                     NOTE -- Production upgrade path:
--                     Add a high-watermark (LastModifiedAt) column to
--                     source tables and filter on etl.ETL_Control table.
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- etl.usp_Load_Dim_Vendor
-- Tracks changes to: VendorName, ContactName, City, PaymentTerms, VendorStatus
-- ────────────────────────────────────────────────────────────
CREATE   PROCEDURE etl.usp_Load_Dim_Vendor
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Today      DATE        = CAST(GETDATE() AS DATE);
    DECLARE @Expired    INT         = 0;
    DECLARE @Inserted   INT         = 0;

    -- Phase 1: Expire rows whose tracked attributes have changed
    UPDATE dw
    SET    dw.EffectiveEnd  = DATEADD(DAY, -1, @Today),
           dw.IsCurrent     = 0,
           dw.DW_UpdateDate = SYSUTCDATETIME()
    FROM   dbo.Dim_Vendor dw
    INNER JOIN PTP_System.Master.Vendors src
           ON  dw.Vendor_BK    = src.VendorCode
          AND  dw.IsCurrent    = 1
    WHERE  dw.SourceRowHash   <> HASHBYTES('SHA2_256',
                                    CONCAT_WS('|',
                                        src.VendorName,
                                        ISNULL(src.ContactName,''),
                                        ISNULL(src.City,''),
                                        src.PaymentTerms,
                                        src.VendorStatus));

    SET @Expired = @@ROWCOUNT;

    -- Phase 2: Insert net-new BKs and new versions of expired rows
    INSERT INTO dbo.Dim_Vendor
        (Vendor_BK, VendorName, ContactName, City, Country, TaxID,
         PaymentTerms, VendorStatus,
         EffectiveStart, EffectiveEnd, IsCurrent, DW_InsertDate, SourceRowHash)
    SELECT
        src.VendorCode,
        src.VendorName,
        ISNULL(src.ContactName,  'Unknown'),
        ISNULL(src.City,         'Unknown'),
        ISNULL(src.Country,      'Unknown'),
        ISNULL(src.TaxID,        'Unknown'),
        src.PaymentTerms,
        src.VendorStatus,
        @Today,
        '9999-12-31',
        1,
        SYSUTCDATETIME(),
        HASHBYTES('SHA2_256',
            CONCAT_WS('|',
                src.VendorName,
                ISNULL(src.ContactName,''),
                ISNULL(src.City,''),
                src.PaymentTerms,
                src.VendorStatus))
    FROM PTP_System.Master.Vendors src
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.Dim_Vendor dw
        WHERE  dw.Vendor_BK = src.VendorCode
          AND  dw.IsCurrent = 1
    );

    SET @Inserted = @@ROWCOUNT;
    PRINT CONCAT('usp_Load_Dim_Vendor: ', @Inserted, ' inserted, ', @Expired, ' expired.');
END;
GO
CREATE   PROCEDURE etl.usp_Load_Fact_AP_Invoice
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dbo.Fact_AP_Invoice;

    INSERT INTO dbo.Fact_AP_Invoice (
        Invoice_BK, Vendor_Invoice_Number, Invoice_Line_Number, PO_BK,
        InvoiceDate_SK, ReceivedDate_SK, DueDate_SK,
        Vendor_SK, Product_SK,
        Invoice_Status, Match_Status,
        Is_Match_Passed, Is_Disputed, Is_Paid,
        Quantity_Billed, Invoiced_Unit_Price, PO_Unit_Price,
        Invoice_Line_Total, PO_Line_Amount,
        Price_Variance, Price_Variance_Pct,
        Days_Until_Due, Days_Invoice_To_Receive
    )
    SELECT
        inv.InternalInvoiceRef,
        inv.InvoiceNumber,
        il.LineNumber,
        po.PONumber,
        -- Date keys
        ISNULL(d_inv.Date_SK, 0)                                        AS InvoiceDate_SK,
        ISNULL(d_rcv.Date_SK, 0)                                        AS ReceivedDate_SK,
        ISNULL(d_due.Date_SK, 0)                                        AS DueDate_SK,
        -- Conformed dimensions
        ISNULL(vnd.Vendor_SK,  -1)                                      AS Vendor_SK,
        ISNULL(prod.Product_SK,-1)                                      AS Product_SK,
        -- AP match flags
        inv.InvoiceStatus,
        inv.MatchStatus,
        CASE WHEN inv.MatchStatus = 'Matched'  THEN 1 ELSE 0 END        AS Is_Match_Passed,
        CASE WHEN inv.InvoiceStatus = 'Disputed' THEN 1 ELSE 0 END      AS Is_Disputed,
        CASE WHEN inv.InvoiceStatus = 'Paid'     THEN 1 ELSE 0 END      AS Is_Paid,
        -- Billing quantities and prices
        il.QuantityBilled,
        il.InvoicedUnitPrice,
        pol.AgreedUnitPrice                                              AS PO_Unit_Price,
        CAST(il.QuantityBilled * il.InvoicedUnitPrice AS DECIMAL(18,2)) AS Invoice_Line_Total,
        CAST(il.QuantityBilled * pol.AgreedUnitPrice  AS DECIMAL(18,2)) AS PO_Line_Amount,
        -- KEY TRANSFORMATION: Price_Variance
        --   Formula: (InvoicePrice - POPrice) x QuantityBilled
        CAST((il.InvoicedUnitPrice - pol.AgreedUnitPrice)
             * il.QuantityBilled AS DECIMAL(18,2))                       AS Price_Variance,
        -- Percentage variance (NULL-safe: no division by zero)
        CASE
            WHEN pol.AgreedUnitPrice <> 0
            THEN CAST(((il.InvoicedUnitPrice - pol.AgreedUnitPrice)
                       / pol.AgreedUnitPrice) * 100 AS DECIMAL(8,4))
            ELSE NULL
        END                                                              AS Price_Variance_Pct,
        -- AP urgency KPI: days from invoice date to due date
        DATEDIFF(DAY, inv.InvoiceDate, inv.DueDate)                     AS Days_Until_Due,
        -- AP processing lag: how many days between invoice date and AP team receiving it
        DATEDIFF(DAY, inv.InvoiceDate, inv.ReceivedDate)                AS Days_Invoice_To_Receive

    FROM PTP_System.Finance.Invoices                                    inv
    INNER JOIN PTP_System.Finance.InvoiceLines                          il
           ON  il.InvoiceID          = inv.InvoiceID
    INNER JOIN PTP_System.Purchasing.PurchaseOrderLines                 pol
           ON  pol.POLineID          = il.POLineID
    INNER JOIN PTP_System.Purchasing.PurchaseOrders                     po
           ON  po.POID               = pol.POID
    INNER JOIN PTP_System.Master.Vendors                                src_vnd
           ON  src_vnd.VendorID      = inv.VendorID
    INNER JOIN PTP_System.Master.Items                                  src_itm
           ON  src_itm.ItemID        = il.ItemID
    -- Date lookups
    LEFT  JOIN dbo.Dim_Date                                             d_inv
           ON  d_inv.Date_SK         = CAST(FORMAT(inv.InvoiceDate,  'yyyyMMdd') AS INT)
    LEFT  JOIN dbo.Dim_Date                                             d_rcv
           ON  d_rcv.Date_SK         = CAST(FORMAT(inv.ReceivedDate, 'yyyyMMdd') AS INT)
    LEFT  JOIN dbo.Dim_Date                                             d_due
           ON  d_due.Date_SK         = CAST(FORMAT(inv.DueDate,      'yyyyMMdd') AS INT)
    -- Conformed dim lookups (current version)
    LEFT  JOIN dbo.Dim_Vendor                                           vnd
           ON  vnd.Vendor_BK         = src_vnd.VendorCode
          AND  vnd.IsCurrent         = 1
    LEFT  JOIN dbo.Dim_Product                                          prod
           ON  prod.Product_BK       = src_itm.ItemCode
          AND  prod.IsCurrent        = 1;

    PRINT CONCAT('usp_Load_Fact_AP_Invoice: ', @@ROWCOUNT, ' rows loaded.');
END;
GO
CREATE   PROCEDURE etl.usp_Load_Fact_Inventory_Receipt
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dbo.Fact_Inventory_Receipt;

    -- CTE computes the running cumulative received quantity per PO line,
    -- ordered chronologically by receipt date and GR primary key.
    ;WITH GR_RunningTotal AS (
        SELECT
            grl.GRLineID,
            grl.GRID,
            grl.POLineID,
            grl.QuantityReceived,
            SUM(grl.QuantityReceived) OVER (
                PARTITION BY grl.POLineID
                ORDER BY     gr_inner.ReceiptDate ASC,
                             gr_inner.GRID        ASC,
                             grl.GRLineID         ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )                                AS CumulativeQtyReceived
        FROM PTP_System.Purchasing.GoodsReceiptLines  grl
        INNER JOIN PTP_System.Purchasing.GoodsReceipts gr_inner
               ON  gr_inner.GRID = grl.GRID
        WHERE  gr_inner.GRStatus = 'Posted'  -- Exclude reversed/cancelled receipts
    )
    INSERT INTO dbo.Fact_Inventory_Receipt (
        GR_BK, PO_BK,
        ReceiptDate_SK, ExpectedDelivery_SK,
        Vendor_SK, Product_SK, Location_SK, ReceivedBy_Employee_SK,
        GR_Status,
        Quantity_Ordered, Quantity_This_Receipt, Cumulative_Qty_Received,
        Quantity_Outstanding, Receipt_Completeness_Pct,
        Lead_Time_Days, Delivery_Variance_Days
    )
    SELECT
        gr.GRNumber                                                     AS GR_BK,
        po.PONumber                                                     AS PO_BK,
        -- Date keys
        ISNULL(d_rcv.Date_SK, 0)                                        AS ReceiptDate_SK,
        ISNULL(d_exp.Date_SK, 0)                                        AS ExpectedDelivery_SK,
        -- Conformed dimensions
        ISNULL(vnd.Vendor_SK,  -1)                                      AS Vendor_SK,
        ISNULL(prod.Product_SK,-1)                                      AS Product_SK,
        ISNULL(loc.Location_SK,-1)                                      AS Location_SK,
        ISNULL(emp.Employee_SK,-1)                                      AS ReceivedBy_Employee_SK,
        gr.GRStatus,
        -- Quantity measures
        pol.Quantity                                                     AS Quantity_Ordered,
        run.QuantityReceived                                             AS Quantity_This_Receipt,
        run.CumulativeQtyReceived                                        AS Cumulative_Qty_Received,
        CAST(pol.Quantity - run.CumulativeQtyReceived AS DECIMAL(10,3)) AS Quantity_Outstanding,
        -- KPI: what % of the PO line has now been fulfilled?
        CAST((run.CumulativeQtyReceived / pol.Quantity) * 100
             AS DECIMAL(5,2))                                            AS Receipt_Completeness_Pct,
        -- KEY TRANSFORMATION 1: Lead time (total supply chain time)
        DATEDIFF(DAY, po.PODate, gr.ReceiptDate)                        AS Lead_Time_Days,
        -- KEY TRANSFORMATION 2: Delivery variance
        --   Positive = late  |  Negative = early  |  NULL = no date set
        CASE
            WHEN po.ExpectedDelivery IS NOT NULL
            THEN DATEDIFF(DAY, po.ExpectedDelivery, gr.ReceiptDate)
            ELSE NULL
        END                                                             AS Delivery_Variance_Days

    FROM GR_RunningTotal                                                run
    INNER JOIN PTP_System.Purchasing.GoodsReceiptLines                  grl
           ON  grl.GRLineID          = run.GRLineID
    INNER JOIN PTP_System.Purchasing.GoodsReceipts                      gr
           ON  gr.GRID               = run.GRID
    INNER JOIN PTP_System.Purchasing.PurchaseOrderLines                 pol
           ON  pol.POLineID          = run.POLineID
    INNER JOIN PTP_System.Purchasing.PurchaseOrders                     po
           ON  po.POID               = pol.POID
    INNER JOIN PTP_System.Master.Vendors                                src_vnd
           ON  src_vnd.VendorID      = po.VendorID
    INNER JOIN PTP_System.Master.Items                                  src_itm
           ON  src_itm.ItemID        = pol.ItemID
    INNER JOIN PTP_System.Master.Employees                              src_emp
           ON  src_emp.EmployeeID    = gr.ReceivedByID
    -- Date lookups
    LEFT  JOIN dbo.Dim_Date                                             d_rcv
           ON  d_rcv.Date_SK         = CAST(FORMAT(gr.ReceiptDate,       'yyyyMMdd') AS INT)
    LEFT  JOIN dbo.Dim_Date                                             d_exp
           ON  d_exp.Date_SK         = CAST(FORMAT(po.ExpectedDelivery,  'yyyyMMdd') AS INT)
    -- Conformed dim lookups
    LEFT  JOIN dbo.Dim_Vendor                                           vnd
           ON  vnd.Vendor_BK         = src_vnd.VendorCode
          AND  vnd.IsCurrent         = 1
    LEFT  JOIN dbo.Dim_Product                                          prod
           ON  prod.Product_BK       = src_itm.ItemCode
          AND  prod.IsCurrent        = 1
    LEFT  JOIN dbo.Dim_Employee                                         emp
           ON  emp.Employee_BK       = src_emp.EmployeeCode
          AND  emp.IsCurrent         = 1
    -- Rule-based location mapping (substitute for source location master)
    LEFT  JOIN dbo.Dim_Location                                         loc
           ON  loc.Location_BK = CASE
                                     WHEN src_itm.ItemCategory = 'Office'
                                          THEN 'WH-SUPP-03'
                                     WHEN src_itm.ItemCategory = 'Hardware'
                                          AND src_itm.ItemCode LIKE '%SWITCH%'
                                          THEN 'WH-NET-04'
                                     WHEN src_itm.ItemCategory = 'Hardware'
                                          THEN 'WH-IT-02'
                                     ELSE 'WH-CORP-01'
                                 END
          AND  loc.IsCurrent   = 1
    WHERE gr.GRStatus = 'Posted';

    PRINT CONCAT('usp_Load_Fact_Inventory_Receipt: ', @@ROWCOUNT, ' rows loaded.');
END;
GO
CREATE   PROCEDURE etl.usp_Load_Fact_PO
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dbo.Fact_PurchaseOrder;

    INSERT INTO dbo.Fact_PurchaseOrder (
        PO_BK, PO_Line_Number, Requisition_BK,
        PODate_SK, ExpectedDelivery_SK,
        Vendor_SK, Product_SK,
        Buyer_Employee_SK, Approver_Employee_SK,
        PO_Status,
        Quantity_Ordered, Agreed_Unit_Price, PO_Line_Total,
        Estimated_Unit_Price, Price_Savings_vs_Estimate,
        Approval_Duration_Days
    )
    SELECT
        po.PONumber,
        pol.LineNumber,
        req.RequisitionNumber,
        -- Date dimension
        ISNULL(d_po.Date_SK,   0)                                       AS PODate_SK,
        ISNULL(d_exp.Date_SK,  0)                                       AS ExpectedDelivery_SK,
        -- Conformed dimensions
        ISNULL(vnd.Vendor_SK,  -1)                                      AS Vendor_SK,
        ISNULL(prod.Product_SK,-1)                                      AS Product_SK,
        ISNULL(emp_buyer.Employee_SK,    -1)                            AS Buyer_Employee_SK,
        ISNULL(emp_approver.Employee_SK, -1)                            AS Approver_Employee_SK,
        po.POStatus,
        -- Core measures
        pol.Quantity,
        pol.AgreedUnitPrice,
        CAST(pol.Quantity * pol.AgreedUnitPrice AS DECIMAL(18,2)),
        -- Negotiation effectiveness measures (from upstream requisition line)
        rl.EstimatedUnitPrice,
        -- Positive = savings achieved vs the budget estimate
        CASE
            WHEN rl.EstimatedUnitPrice IS NOT NULL
            THEN CAST((rl.EstimatedUnitPrice - pol.AgreedUnitPrice) * pol.Quantity AS DECIMAL(18,2))
            ELSE NULL
        END                                                             AS Price_Savings_vs_Estimate,
        -- Procurement lead time: days from requisition date to PO being raised
        CASE
            WHEN req.RequisitionDate IS NOT NULL
            THEN DATEDIFF(DAY, req.RequisitionDate, po.PODate)
            ELSE NULL
        END                                                             AS Approval_Duration_Days

    FROM PTP_System.Purchasing.PurchaseOrders              po
    INNER JOIN PTP_System.Purchasing.PurchaseOrderLines    pol
           ON  pol.POID                  = po.POID
    INNER JOIN PTP_System.Master.Vendors                   src_vnd
           ON  src_vnd.VendorID          = po.VendorID
    INNER JOIN PTP_System.Master.Items                     src_itm
           ON  src_itm.ItemID            = pol.ItemID
    INNER JOIN PTP_System.Master.Employees                 src_buyer
           ON  src_buyer.EmployeeID      = po.OrderedByID
    -- Optional upstream requisition
    LEFT  JOIN PTP_System.Purchasing.Requisitions          req
           ON  req.RequisitionID         = po.RequisitionID
    LEFT  JOIN PTP_System.Purchasing.RequisitionLines      rl
           ON  rl.RequisitionLineID      = pol.RequisitionLineID
    -- Approver (optional)
    LEFT  JOIN PTP_System.Master.Employees                 src_approver
           ON  src_approver.EmployeeID   = po.ApprovedByID
    -- Date dimension lookups
    LEFT  JOIN dbo.Dim_Date                                d_po
           ON  d_po.Date_SK              = CAST(FORMAT(po.PODate,          'yyyyMMdd') AS INT)
    LEFT  JOIN dbo.Dim_Date                                d_exp
           ON  d_exp.Date_SK             = CAST(FORMAT(po.ExpectedDelivery,'yyyyMMdd') AS INT)
    -- Conformed dimension lookups (current SCD2 version)
    LEFT  JOIN dbo.Dim_Vendor                              vnd
           ON  vnd.Vendor_BK             = src_vnd.VendorCode
          AND  vnd.IsCurrent             = 1
    LEFT  JOIN dbo.Dim_Product                             prod
           ON  prod.Product_BK           = src_itm.ItemCode
          AND  prod.IsCurrent            = 1
    LEFT  JOIN dbo.Dim_Employee                            emp_buyer
           ON  emp_buyer.Employee_BK     = src_buyer.EmployeeCode
          AND  emp_buyer.IsCurrent       = 1
    LEFT  JOIN dbo.Dim_Employee                            emp_approver
           ON  emp_approver.Employee_BK  = src_approver.EmployeeCode
          AND  emp_approver.IsCurrent    = 1;

    PRINT CONCAT('usp_Load_Fact_PO: ', @@ROWCOUNT, ' rows loaded.');
END;
GO
CREATE   PROCEDURE etl.usp_Load_Fact_Requisition
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dbo.Fact_Requisition;

    INSERT INTO dbo.Fact_Requisition (
        Requisition_BK, RequisitionLine_BK,
        RequisitionDate_SK, RequiredByDate_SK,
        Requestor_Employee_SK, Approver_Employee_SK,
        Product_SK, SuggestedVendor_SK,
        Requisition_Status, Is_Approved, Is_Rejected, Is_Converted_To_PO,
        Quantity_Requested, Estimated_Unit_Price, Estimated_Line_Total,
        Approval_Duration_Days
    )
    SELECT
        r.RequisitionNumber,
        rl.RequisitionLineID,
        -- Date dimension lookups using YYYYMMDD integer key
        ISNULL(d_req.Date_SK, 0)                                        AS RequisitionDate_SK,
        ISNULL(d_rby.Date_SK, 0)                                        AS RequiredByDate_SK,
        -- Employee lookups (join source -> current DW dimension row)
        ISNULL(emp_req.Employee_SK, -1)                                 AS Requestor_Employee_SK,
        ISNULL(emp_apr.Employee_SK, -1)                                 AS Approver_Employee_SK,
        ISNULL(prod.Product_SK, -1)                                     AS Product_SK,
        ISNULL(svnd.Vendor_SK,  -1)                                     AS SuggestedVendor_SK,
        -- Status flags
        r.RequisitionStatus,
        CASE WHEN r.RequisitionStatus IN ('Approved','Converted') THEN 1 ELSE 0 END,
        CASE WHEN r.RequisitionStatus = 'Rejected'                THEN 1 ELSE 0 END,
        CASE WHEN r.RequisitionStatus = 'Converted'               THEN 1 ELSE 0 END,
        -- Measures
        rl.Quantity,
        rl.EstimatedUnitPrice,
        CAST(rl.Quantity * rl.EstimatedUnitPrice AS DECIMAL(18,2)),
        -- Approval duration: days from submission to actioning
        CASE
            WHEN r.RequisitionStatus IN ('Approved','Rejected','Converted')
            THEN DATEDIFF(DAY, r.RequisitionDate, CAST(r.UpdatedAt AS DATE))
            ELSE NULL
        END

    FROM PTP_System.Purchasing.Requisitions             r
    INNER JOIN PTP_System.Purchasing.RequisitionLines   rl
           ON  rl.RequisitionID          = r.RequisitionID
    -- Source-side employee and item joins
    INNER JOIN PTP_System.Master.Employees              src_req_emp
           ON  src_req_emp.EmployeeID    = r.RequestedByID
    INNER JOIN PTP_System.Master.Items                  src_itm
           ON  src_itm.ItemID            = rl.ItemID
    -- Date dimension
    LEFT  JOIN dbo.Dim_Date                             d_req
           ON  d_req.Date_SK             = CAST(FORMAT(r.RequisitionDate,  'yyyyMMdd') AS INT)
    LEFT  JOIN dbo.Dim_Date                             d_rby
           ON  d_rby.Date_SK             = CAST(FORMAT(r.RequiredByDate,   'yyyyMMdd') AS INT)
    -- Employee dimension (current version)
    LEFT  JOIN dbo.Dim_Employee                         emp_req
           ON  emp_req.Employee_BK       = src_req_emp.EmployeeCode
          AND  emp_req.IsCurrent         = 1
    LEFT  JOIN PTP_System.Master.Employees              src_apr_emp
           ON  src_apr_emp.EmployeeID    = r.ApprovedByID
    LEFT  JOIN dbo.Dim_Employee                         emp_apr
           ON  emp_apr.Employee_BK       = src_apr_emp.EmployeeCode
          AND  emp_apr.IsCurrent         = 1
    -- Product dimension (current version)
    LEFT  JOIN dbo.Dim_Product                          prod
           ON  prod.Product_BK           = src_itm.ItemCode
          AND  prod.IsCurrent            = 1
    -- Suggested vendor dimension (current version)
    LEFT  JOIN PTP_System.Master.Vendors                src_svnd
           ON  src_svnd.VendorID         = rl.SuggestedVendorID
    LEFT  JOIN dbo.Dim_Vendor                           svnd
           ON  svnd.Vendor_BK            = src_svnd.VendorCode
          AND  svnd.IsCurrent            = 1;

    PRINT CONCAT('usp_Load_Fact_Requisition: ', @@ROWCOUNT, ' rows loaded.');
END;
GO
