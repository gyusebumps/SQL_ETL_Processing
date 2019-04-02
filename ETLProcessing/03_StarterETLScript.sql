--*************************************************************************--
-- Title: Assignment02
-- Author: <GyubeomKim>
-- Desc: This file tests you knowlege on how to create a ETL process with SQL code
-- Change Log: When,Who,What
-- 2018-07-30,<GyubeomKim>,Created File
-- 2018-07-30,<GyuboemKim>,added error messages for the status

-- Instructions: 
-- (STEP 1) Create a lite version of the Northwind database by running the provided code.
-- (STEP 2) Create a new Data Warehouse called DWNorthwindLite based on the NorthwindLite DB.
--          The DW should have three dimension tables (for Customers, Products, and Dates) and one fact table.
-- (STEP 3) Fill the DW by creating an ETL Script
--**************************************************************************--
USE [DWNorthwindLite];
go
SET NoCount ON;
go
	If Exists(Select * from Sys.objects where Name = 'pETLDropForeignKeyConstraints')
   Drop Procedure pETLDropForeignKeyConstraints;
go
	If Exists(Select * from Sys.objects where Name = 'pETLTruncateTables')
   Drop Procedure pETLTruncateTables;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProducts')
   Drop View vETLDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimProducts')
   Drop Procedure pETLFillDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimCustomers')
   Drop View vETLDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimCustomers')
   Drop Procedure pETLFillDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactOrders')
   Drop View vETLFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillFactOrders')
   Drop Procedure pETLFillFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLAddForeignKeyConstraints')
   Drop Procedure pETLAddForeignKeyConstraints;

--********************************************************************--
-- A) Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--
go
Create Procedure pETLDropForeignKeyConstraints
/* Author: <GyubeomKim>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Alter Table [DWNorthwindLite].dbo.FactOrders
	  Drop Constraint [fkFactOrdersToDimProducts]; 

	ALTER TABLE DWNorthwindLite.dbo.FactOrders
	  Drop CONSTRAINT fkFactOrdersToDimCustomers;

    -- Optional: Unlike the other tables DimDates does not change often --
    Alter Table [DWNorthwindLite].dbo.FactOrders
	   Drop Constraint [fkFactOrdersToDimDates];
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/*Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropForeignKeyConstraints;
 Print @Status;
*/
go

Create Procedure pETLTruncateTables
/* Author: <GyubeomKim>
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Truncate Table [DWNorthwindLite].dbo.DimProducts;
    Truncate Table [DWNorthwindLite].dbo.DimCustomers;
	Truncate Table [DWNorthwindLite].dbo.FactOrders;  
    -- Optional: Unlike the other tables DimDates does not change often --
    Truncate Table [DWNorthwindLite].dbo.DimDates; 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/*Testing Code:
 Declare @Status int;
 Exec @Status = pETLTruncateTables;
 Print @Status;
*/
go

--********************************************************************--
-- B) FILL the Tables
--********************************************************************--
/****** [dbo].[DimProducts] ******/
go 
Create View vETLDimProducts
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
  FROM [NorthwindLite].dbo.Categories as c
  INNER JOIN [NorthwindLite].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLFillDimProducts
/* Author: <GyubeomKim>
** Desc: Inserts data into DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From DimProducts) = 0)
     Begin
      INSERT INTO [DWNorthwindLite].dbo.DimProducts
      ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [ProductID]
       ,[ProductName]
       ,[ProductCategoryID]
       ,[ProductCategoryName]
       ,[StartDate] = -1
       ,[EndDate] = Null -- Default
       ,[IsCurrent] = 'Yes' -- Default
      FROM vETLDimProducts
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimProducts;
 Print @Status;
 Select * From DimProducts;
*/


/****** [dbo].[DimCustomers] ******/
go 
Create View vETLDimCustomers
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
As
    SELECT
    [CustomerID] = CAST(cu.CustomerID as nchar(5))
   ,[CustomerName] = CAST(cu.CompanyName as nVarchar(100))
   ,[CustomerCity] = CAST(cu.City as nVarchar(100))
   ,[CustomerCountry] = CAST(cu.Country as nVarchar(100))
  FROM [NorthwindLite].dbo.Customers as cu
go

/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLFillDimCustomers
/* Author: <GyubeomKim>
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From DimCustomers) = 0)
     Begin
      INSERT INTO [DWNorthwindLite].dbo.DimCustomers
      ([CustomerID], [CustomerName], [CustomerCity], [CustomerCountry], [StartDate], [EndDate], [IsCurrent])
      SELECT
        [CustomerID]
       ,[CustomerName]
       ,[CustomerCity]
	   ,[CustomerCountry]
       ,[StartDate] = -1
       ,[EndDate] = Null -- Default
       ,[IsCurrent] = 'Yes' -- Default
      FROM vETLDimCustomers
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimCustomers;
 Print @Status;
 Select * From DimCustomers;
*/
go

/****** [dbo].[DimDates] ******/
Create Procedure pETLFillDimDates
/* Author: <GyubeomKim>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/1990'
      Declare @EndDate datetime = '01/01/2020' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       Insert Into DimDates 
       ( [DateKey], [USADateName], [MonthKey], [MonthName], [QuarterKey], [QuarterName], [YearKey], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(100), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthKey]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Cast(DateName(YYYY,@DateInProcess) + '0' + (DateName(quarter, @DateInProcess) ) as int)  -- [QuarterKey]
        ,'Q' + DateName(quarter, @DateInProcess) + ' - ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
        ,Year(@DateInProcess) -- [YearKey] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
*/
go

/****** [dbo].[FactOrders] ******/
go 
Create View vETLFactOrders
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [OrderID] = Cast(o.OrderID as int)
   ,[CustomerKey] = Cast(dc.CustomerKey as int)
   ,[OrderDateKey] = Cast(dd.DateKey as int)
   ,[ProductKey] = Cast(dp.ProductKey as int)
   ,[ActualOrderUnitPrice] = Cast(od.UnitPrice as money)
   ,[ActualOrderQuantity] = Cast(od.Quantity as int)
  FROM [NorthwindLite].dbo.OrderDetails as od
  JOIN [NorthwindLite].dbo.Orders as o
  ON od.OrderID = o.OrderID
  JOIN [DWNorthwindLite].dbo.DimCustomers as dc
  On dc.CustomerID = o.CustomerID
  JOIN [DWNorthwindLite].dbo.DimProducts as dp
  On od.ProductID = dp.ProductID
  JOIN [DWNorthwindLite].dbo.DimDates as dd
  On Cast(Convert(nVarchar(50), o.OrderDate, 112) as int) = dd.DateKey;
go
/* Testing Code:
 Select * From vETLFactOrders;
*/

go
Create Procedure pETLFillFactOrders
/* Author: <GyubeomKim>
** Desc: Inserts data into FactOrders
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     Begin
	  IF ((Select Count(*) From FactOrders) = 0)
      INSERT INTO [DWNorthwindLite].dbo.FactOrders
      ([OrderID], [CustomerKey], [OrderDateKey], [ProductKey], [ActualOrderUnitPrice], [ActualOrderQuantity])
      SELECT
        [OrderID]
       ,[CustomerKey]
       ,[OrderDateKey]
	   ,[ProductKey]
	   ,[ActualOrderUnitPrice]
       ,[ActualOrderQuantity]
      FROM vETLFactOrders
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillFactOrders;
 Print @Status;
  Select * From FactOrders;
*/
go

--********************************************************************--
-- C) Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
go
Create Procedure pETLAddForeignKeyConstraints
/* Author: <GyubeomKim>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 2018-07-26,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
      ADD CONSTRAINT fkFactOrdersToDimProducts
      FOREIGN KEY (ProductKey) REFERENCES DimProducts(ProductKey);

	ALTER TABLE DWNorthwindLite.dbo.FactOrders
	  ADD CONSTRAINT fkFactOrdersToDimCustomers
	  FOREIGN KEY (CustomerKey) REFERENCES DimCustomers(CustomerKey);

    -- Optional: Unlike the other tables DimDates does not change often --
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
      ADD CONSTRAINT fkFactOrdersToDimDates 
      FOREIGN KEY (OrderDateKey) REFERENCES DimDates(DateKey);
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLAddForeignKeyConstraints;
 Print @Status;
*/
go

--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int;
Exec @Status = pETLDropForeignKeyConstraints;
Select [Object] = 'pETLDropForeignKeyConstraints', [Status] = Case @Status
	  When +1 Then 'ETL to Drop Foriegn Keys was successful!'
	  When -1 Then 'ETL to Drop Foriegn Keys failed! Common Issues: FKs have already been dropped'
	  End

Exec @Status = pETLTruncateTables;
Select [Object] = 'pETLTruncateTables', [Status] = Case @Status
	  When +1 Then 'ETL to Truncate Tables was successful!'
	  When -1 Then 'ETL to Truncate Tables failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimProducts;
Select [Object] = 'pETLFillDimProducts', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimProducts table was successful!'
	  When -1 Then 'ETL to Fill DimProducts table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimCustomers;
Select [Object] = 'pETLFillDimCustomers', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimCustomers table was successful!'
	  When -1 Then 'ETL to Fill DimCustomers table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimDates table was successful!'
	  When -1 Then 'ETL to Fill DimDates table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillFactOrders;
Select [Object] = 'pETLFillFactOrders', [Status] = Case @Status
	  When +1 Then 'ETL to Fill FactOrders table was successful!'
	  When -1 Then 'ETL to Drop FactOrders table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLAddForeignKeyConstraints;
Select [Object] = 'pETLAddForeignKeyConstraints', [Status] = Case @Status
	  When +1 Then 'ETL to add Foriegn Keys was successful!'
	  When -1 Then 'ETL to add Foriegn Keys failed! Common Issues: FKs have already been added'
	  End
go

Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactOrders];