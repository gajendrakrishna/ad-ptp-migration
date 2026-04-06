CREATE TABLE [dbo].[Dim_Date] (
    [Date_SK] INT NOT NULL,
    [FullDate] DATE NOT NULL,
    [DayName] VARCHAR(20) NOT NULL,
    [DayOfWeek] TINYINT NOT NULL,
    [DayOfMonth] TINYINT NOT NULL,
    [DayOfYear] SMALLINT NOT NULL,
    [WeekOfYear] TINYINT NOT NULL,
    [MonthNumber] TINYINT NOT NULL,
    [MonthName] VARCHAR(20) NOT NULL,
    [MonthShort] CHAR(3) NOT NULL,
    [CalendarQuarter] TINYINT NOT NULL,
    [CalendarQuarterName] CHAR(2) NOT NULL,
    [CalendarYear] SMALLINT NOT NULL,
    [CalendarYearMonth] INT NOT NULL,
    [FiscalYear] SMALLINT NOT NULL,
    [FiscalQuarter] TINYINT NOT NULL,
    [FiscalMonth] TINYINT NOT NULL,
    [IsWeekend] BIT NOT NULL,
    [IsHoliday] BIT NOT NULL,
    [HolidayName] VARCHAR(60) NULL
)
GO
CREATE TABLE [dbo].[Dim_Employee] (
    [Employee_SK] INT IDENTITY(1,1) NOT NULL,
    [Employee_BK] VARCHAR(20) NOT NULL,
    [FullName] NVARCHAR(100) NOT NULL,
    [Email] NVARCHAR(150) NOT NULL,
    [Department] NVARCHAR(50) NOT NULL,
    [JobTitle] NVARCHAR(100) NOT NULL,
    [IsActive] BIT NOT NULL,
    [EffectiveStart] DATE NOT NULL,
    [EffectiveEnd] DATE NOT NULL,
    [IsCurrent] BIT NOT NULL,
    [DW_InsertDate] DATETIME2 NOT NULL,
    [SourceRowHash] VARBINARY(32) NULL
)
GO
CREATE TABLE [dbo].[Dim_Location] (
    [Location_SK] INT IDENTITY(1,1) NOT NULL,
    [Location_BK] VARCHAR(20) NOT NULL,
    [LocationName] NVARCHAR(100) NOT NULL,
    [LocationType] VARCHAR(30) NOT NULL,
    [AddressLine1] NVARCHAR(200) NULL,
    [City] NVARCHAR(100) NOT NULL,
    [StateProvince] VARCHAR(50) NULL,
    [Country] NVARCHAR(100) NOT NULL,
    [EffectiveStart] DATE NOT NULL,
    [EffectiveEnd] DATE NOT NULL,
    [IsCurrent] BIT NOT NULL,
    [DW_InsertDate] DATETIME2 NOT NULL
)
GO
CREATE TABLE [dbo].[Dim_Product] (
    [Product_SK] INT IDENTITY(1,1) NOT NULL,
    [Product_BK] VARCHAR(30) NOT NULL,
    [ProductName] NVARCHAR(200) NOT NULL,
    [Description] NVARCHAR(500) NOT NULL,
    [UnitOfMeasure] VARCHAR(20) NOT NULL,
    [Category] VARCHAR(30) NOT NULL,
    [AccountingType] VARCHAR(20) NOT NULL,
    [GLAccount] VARCHAR(20) NOT NULL,
    [EffectiveStart] DATE NOT NULL,
    [EffectiveEnd] DATE NOT NULL,
    [IsCurrent] BIT NOT NULL,
    [DW_InsertDate] DATETIME2 NOT NULL,
    [SourceRowHash] VARBINARY(32) NULL
)
GO
CREATE TABLE [dbo].[Dim_Vendor] (
    [Vendor_SK] INT IDENTITY(1,1) NOT NULL,
    [Vendor_BK] VARCHAR(20) NOT NULL,
    [VendorName] NVARCHAR(150) NOT NULL,
    [ContactName] NVARCHAR(100) NOT NULL,
    [City] NVARCHAR(100) NOT NULL,
    [Country] NVARCHAR(100) NOT NULL,
    [TaxID] VARCHAR(50) NOT NULL,
    [PaymentTerms] VARCHAR(20) NOT NULL,
    [VendorStatus] VARCHAR(20) NOT NULL,
    [EffectiveStart] DATE NOT NULL,
    [EffectiveEnd] DATE NOT NULL,
    [IsCurrent] BIT NOT NULL,
    [DW_InsertDate] DATETIME2 NOT NULL,
    [DW_UpdateDate] DATETIME2 NOT NULL,
    [SourceRowHash] VARBINARY(32) NULL
)
GO
CREATE TABLE [dbo].[Fact_AP_Invoice] (
    [Fact_Invoice_SK] BIGINT IDENTITY(1,1) NOT NULL,
    [Invoice_BK] VARCHAR(20) NOT NULL,
    [Vendor_Invoice_Number] VARCHAR(50) NOT NULL,
    [Invoice_Line_Number] SMALLINT NOT NULL,
    [PO_BK] VARCHAR(20) NOT NULL,
    [InvoiceDate_SK] INT NOT NULL,
    [ReceivedDate_SK] INT NOT NULL,
    [DueDate_SK] INT NOT NULL,
    [Vendor_SK] INT NOT NULL,
    [Product_SK] INT NOT NULL,
    [Invoice_Status] VARCHAR(20) NOT NULL,
    [Match_Status] VARCHAR(20) NOT NULL,
    [Is_Match_Passed] BIT NOT NULL,
    [Is_Disputed] BIT NOT NULL,
    [Is_Paid] BIT NOT NULL,
    [Quantity_Billed] DECIMAL(10,3) NOT NULL,
    [Invoiced_Unit_Price] DECIMAL(18,4) NOT NULL,
    [PO_Unit_Price] DECIMAL(18,4) NOT NULL,
    [Invoice_Line_Total] DECIMAL(18,2) NOT NULL,
    [PO_Line_Amount] DECIMAL(18,2) NOT NULL,
    [Price_Variance] DECIMAL(18,2) NOT NULL,
    [Price_Variance_Pct] DECIMAL(8,4) NULL,
    [Days_Until_Due] INT NULL,
    [Days_Invoice_To_Receive] INT NULL,
    [DW_Load_DateTime] DATETIME2 NOT NULL
)
GO
CREATE TABLE [dbo].[Fact_Inventory_Receipt] (
    [Fact_Receipt_SK] BIGINT IDENTITY(1,1) NOT NULL,
    [GR_BK] VARCHAR(20) NOT NULL,
    [PO_BK] VARCHAR(20) NOT NULL,
    [ReceiptDate_SK] INT NOT NULL,
    [ExpectedDelivery_SK] INT NOT NULL,
    [Vendor_SK] INT NOT NULL,
    [Product_SK] INT NOT NULL,
    [Location_SK] INT NOT NULL,
    [ReceivedBy_Employee_SK] INT NOT NULL,
    [GR_Status] VARCHAR(20) NOT NULL,
    [Quantity_Ordered] DECIMAL(10,3) NOT NULL,
    [Quantity_This_Receipt] DECIMAL(10,3) NOT NULL,
    [Cumulative_Qty_Received] DECIMAL(10,3) NOT NULL,
    [Quantity_Outstanding] DECIMAL(10,3) NOT NULL,
    [Receipt_Completeness_Pct] DECIMAL(5,2) NOT NULL,
    [Lead_Time_Days] INT NOT NULL,
    [Delivery_Variance_Days] INT NULL,
    [DW_Load_DateTime] DATETIME2 NOT NULL
)
GO
CREATE TABLE [dbo].[Fact_PurchaseOrder] (
    [Fact_PO_SK] BIGINT IDENTITY(1,1) NOT NULL,
    [PO_BK] VARCHAR(20) NOT NULL,
    [PO_Line_Number] SMALLINT NOT NULL,
    [Requisition_BK] VARCHAR(20) NULL,
    [PODate_SK] INT NOT NULL,
    [ExpectedDelivery_SK] INT NOT NULL,
    [Vendor_SK] INT NOT NULL,
    [Product_SK] INT NOT NULL,
    [Buyer_Employee_SK] INT NOT NULL,
    [Approver_Employee_SK] INT NOT NULL,
    [PO_Status] VARCHAR(20) NOT NULL,
    [Quantity_Ordered] DECIMAL(10,3) NOT NULL,
    [Agreed_Unit_Price] DECIMAL(18,4) NOT NULL,
    [PO_Line_Total] DECIMAL(18,2) NOT NULL,
    [Estimated_Unit_Price] DECIMAL(18,4) NULL,
    [Price_Savings_vs_Estimate] DECIMAL(18,2) NULL,
    [Approval_Duration_Days] INT NULL,
    [DW_Load_DateTime] DATETIME2 NOT NULL
)
GO
CREATE TABLE [dbo].[Fact_Requisition] (
    [Fact_Requisition_SK] BIGINT IDENTITY(1,1) NOT NULL,
    [Requisition_BK] VARCHAR(20) NOT NULL,
    [RequisitionLine_BK] INT NOT NULL,
    [RequisitionDate_SK] INT NOT NULL,
    [RequiredByDate_SK] INT NOT NULL,
    [Requestor_Employee_SK] INT NOT NULL,
    [Approver_Employee_SK] INT NOT NULL,
    [Product_SK] INT NOT NULL,
    [SuggestedVendor_SK] INT NOT NULL,
    [Requisition_Status] VARCHAR(20) NOT NULL,
    [Is_Approved] BIT NOT NULL,
    [Is_Rejected] BIT NOT NULL,
    [Is_Converted_To_PO] BIT NOT NULL,
    [Quantity_Requested] DECIMAL(10,3) NOT NULL,
    [Estimated_Unit_Price] DECIMAL(18,4) NOT NULL,
    [Estimated_Line_Total] DECIMAL(18,2) NOT NULL,
    [Approval_Duration_Days] INT NULL,
    [DW_Load_DateTime] DATETIME2 NOT NULL
)
GO
