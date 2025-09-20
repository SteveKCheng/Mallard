-- Schema creation for the Northwind database, adapted to the DuckDB SQL dialect
--
-- Adapted from: https://github.com/jpwhite3/northwind-SQLite3/blob/main/src/create.sql
-- License 1: https://github.com/jpwhite3/northwind-SQLite3/blob/main/LICENSE
--            MIT License, Copyright 2016 JP White
-- License 2: Microsoft Public License, see Northwind-LICENSE.txt 
--

-- TABLE [Categories]
DROP TABLE IF EXISTS Categories;
CREATE TABLE Categories (
    CategoryID INTEGER NOT NULL PRIMARY KEY,
    CategoryName TEXT,
    Description TEXT,
    Picture BLOB
);

-- TABLE [Regions]
DROP TABLE IF EXISTS Regions;
CREATE TABLE Regions (
    RegionID INTEGER NOT NULL PRIMARY KEY,
    RegionDescription TEXT NOT NULL
);

-- TABLE [Territories]
DROP TABLE IF EXISTS Territories;
CREATE TABLE Territories (
    TerritoryID TEXT NOT NULL PRIMARY KEY,
    TerritoryDescription TEXT NOT NULL,
    RegionID INTEGER NOT NULL,
    FOREIGN KEY (RegionID) REFERENCES Regions (RegionID)
        ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- TABLE [Customers]
DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers (
    CustomerID TEXT NOT NULL PRIMARY KEY,
    CompanyName TEXT,
    ContactName TEXT,
    ContactTitle TEXT,
    Address TEXT,
    City TEXT,
    Region TEXT,
    PostalCode TEXT,
    Country TEXT,
    Phone TEXT,
    Fax TEXT
);

-- TABLE [Shippers]
DROP TABLE IF EXISTS Shippers;
CREATE TABLE Shippers (
    ShipperID INTEGER NOT NULL PRIMARY KEY,
    CompanyName TEXT NOT NULL,
    Phone TEXT
);

-- TABLE [Suppliers]
DROP TABLE IF EXISTS Suppliers;
CREATE TABLE Suppliers (
    SupplierID INTEGER NOT NULL PRIMARY KEY,
    CompanyName TEXT NOT NULL,
    ContactName TEXT,
    ContactTitle TEXT,
    Address TEXT,
    City TEXT,
    Region TEXT,
    PostalCode TEXT,
    Country TEXT,
    Phone TEXT,
    Fax TEXT,
    HomePage TEXT
);

-- TABLE [Employee]
DROP TABLE IF EXISTS Employees;
CREATE TABLE Employees (
    EmployeeID INTEGER NOT NULL PRIMARY KEY,
    LastName TEXT,
    FirstName TEXT,
    Title TEXT,
    TitleOfCourtesy TEXT,
    BirthDate DATE,
    HireDate DATE,
    Address TEXT,
    City TEXT,
    Region TEXT,
    PostalCode TEXT,
    Country TEXT,
    HomePhone TEXT,
    Extension TEXT,
    Photo BLOB,
    Notes TEXT,
    ReportsTo INTEGER,
    PhotoPath TEXT,
    FOREIGN KEY (ReportsTo) REFERENCES Employees (EmployeeID)
    ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- TABLE [EmployeeTerritories]
DROP TABLE IF EXISTS EmployeeTerritories;
CREATE TABLE EmployeeTerritories (
    EmployeeID INTEGER NOT NULL,
    TerritoryID TEXT NOT NULL,
    PRIMARY KEY ("EmployeeID","TerritoryID"),
    FOREIGN KEY (EmployeeID) REFERENCES Employees (EmployeeID)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (TerritoryID) REFERENCES Territories (TerritoryID)
        ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- TABLE [CustomerDemographics]
DROP TABLE IF EXISTS CustomerDemographics;
CREATE TABLE CustomerDemographics (
    CustomerTypeID TEXT NOT NULL,
    CustomerDesc TEXT,
    PRIMARY KEY ("CustomerTypeID")
);

-- TABLE [CustomerCustomerDemo]
DROP TABLE IF EXISTS CustomerCustomerDemo;
CREATE TABLE CustomerCustomerDemo (
    CustomerID TEXT NOT NULL,
    CustomerTypeID TEXT NOT NULL,
    PRIMARY KEY ("CustomerID", "CustomerTypeID"),
    FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (CustomerTypeID) REFERENCES CustomerDemographics (CustomerTypeID)
        ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- TABLE [Products]
DROP TABLE IF EXISTS Products;
CREATE TABLE Products (
    ProductID INTEGER NOT NULL PRIMARY KEY,
    ProductName TEXT NOT NULL,
    SupplierID INTEGER,
    CategoryID INTEGER,
    QuantityPerUnit TEXT,
    UnitPrice NUMERIC DEFAULT 0,
    UnitsInStock INTEGER DEFAULT 0,
    UnitsOnOrder INTEGER DEFAULT 0,
    ReorderLevel INTEGER DEFAULT 0,
    Discontinued TEXT NOT NULL DEFAULT '0',
    CHECK (UnitPrice >=(0)),
    CHECK (ReorderLevel >=(0)),
    CHECK (UnitsInStock >=(0)),
    CHECK (UnitsOnOrder >=(0)),
    FOREIGN KEY (CategoryID) REFERENCES Categories (CategoryID)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (SupplierID) REFERENCES Suppliers (SupplierID)
        ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- TABLE [Orders]
DROP TABLE IF EXISTS Orders;
CREATE TABLE Orders (
    OrderID INTEGER NOT NULL PRIMARY KEY,
    CustomerID TEXT,
    EmployeeID INTEGER,
    OrderDate DATETIME,
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    ShipVia INTEGER,
    Freight NUMERIC DEFAULT 0,
    ShipName TEXT,
    ShipAddress TEXT,
    ShipCity TEXT,
    ShipRegion TEXT,
    ShipPostalCode TEXT,
    ShipCountry TEXT,
    FOREIGN KEY (EmployeeID) REFERENCES Employees (EmployeeID)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (ShipVia) REFERENCES Shippers (ShipperID)
        ON DELETE NO ACTION ON UPDATE NO ACTION
);

CREATE INDEX OrderDate ON Orders ("OrderDate" ASC);
CREATE INDEX ShippedDate ON Orders ("ShippedDate" ASC);
CREATE INDEX ShipPostalCode ON Orders ("ShipPostalCode" ASC);

-- TABLE [Order Details]
DROP TABLE IF EXISTS "Order Details";
CREATE TABLE "Order Details" (
    OrderID INTEGER NOT NULL,
    ProductID INTEGER NOT NULL,
    UnitPrice NUMERIC NOT NULL DEFAULT 0,
    Quantity INTEGER NOT NULL DEFAULT 1,
    Discount REAL NOT NULL DEFAULT 0,
    PRIMARY KEY ("OrderID","ProductID"),
    CHECK (Discount >= 0 AND Discount <= 1),
    CHECK (Quantity > 0),
    CHECK (UnitPrice >= 0),
    FOREIGN KEY (OrderID) REFERENCES Orders (OrderID)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (ProductID) REFERENCES Products (ProductID)
        ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- VIEW [Alphabetical list of products]
DROP VIEW IF EXISTS "Alphabetical list of products";
CREATE VIEW "Alphabetical list of products" AS
    SELECT Products.*, Categories.CategoryName
    FROM Categories
    INNER JOIN Products ON Categories.CategoryID = Products.CategoryID
    WHERE (((Products.Discontinued)=0));

-- VIEW [Current Product List]
DROP VIEW IF EXISTS "Current Product List";
CREATE VIEW "Current Product List" AS
    SELECT ProductID, ProductName
    FROM Products
    WHERE Discontinued=0;

-- VIEW [Customer and Suppliers by City]
DROP VIEW IF EXISTS "Customer and Suppliers by City";
CREATE VIEW "Customer and Suppliers by City" AS
    SELECT City, CompanyName, ContactName, 'Customers' AS Relationship
    FROM Customers
    UNION
    SELECT City, CompanyName, ContactName, 'Suppliers'
    FROM Suppliers
    ORDER BY City, CompanyName;

-- VIEW [Invoices]
DROP VIEW IF EXISTS Invoices;
CREATE VIEW Invoices AS
    SELECT
        Orders.ShipName,
        Orders.ShipAddress,
        Orders.ShipCity,
        Orders.ShipRegion,
        Orders.ShipPostalCode,
        Orders.ShipCountry,
        Orders.CustomerID,
        Customers.CompanyName AS CustomerName,
        Customers.Address,
        Customers.City,
        Customers.Region,
        Customers.PostalCode,
        Customers.Country,
        CONCAT(Employees.FirstName, ' ', Employees.LastName) AS Salesperson,
        Orders.OrderID,
        Orders.OrderDate,
        Orders.RequiredDate,
        Orders.ShippedDate,
        Shippers.CompanyName As ShipperName,
        "Order Details".ProductID,
        Products.ProductName,
        "Order Details".UnitPrice,
        "Order Details".Quantity,
        "Order Details".Discount,
        ((("Order Details".UnitPrice*Quantity*(1-Discount))/100)*100) AS ExtendedPrice,
        Orders.Freight
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    JOIN Employees ON Employees.EmployeeID = Orders.EmployeeID
    JOIN "Order Details" ON Orders.OrderID = "Order Details".OrderID
    JOIN Products ON Products.ProductID = "Order Details".ProductID
    JOIN Shippers ON Shippers.ShipperID = Orders.ShipVia;

-- VIEW [Orders Qry]
DROP VIEW IF EXISTS "Orders Qry";
CREATE VIEW "Orders Qry" AS
    SELECT
        Orders.OrderID,
        Orders.CustomerID,
        Orders.EmployeeID,
        Orders.OrderDate,
        Orders.RequiredDate,
        Orders.ShippedDate,
        Orders.ShipVia,
        Orders.Freight,
        Orders.ShipName,
        Orders.ShipAddress,
        Orders.ShipCity,
        Orders.ShipRegion,
        Orders.ShipPostalCode,
        Orders.ShipCountry,
        Customers.CompanyName,
        Customers.Address,
        Customers.City,
        Customers.Region,
        Customers.PostalCode,
        Customers.Country
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID;

-- VIEW [Order Subtotals]
DROP VIEW IF EXISTS "Order Subtotals";
CREATE VIEW "Order Subtotals" AS
    SELECT
        "Order Details".OrderID,
        SUM(("Order Details".UnitPrice*Quantity*(1-Discount)/100)*100) AS Subtotal
    FROM "Order Details"
    GROUP BY "Order Details".OrderID;

-- VIEW [Product Sales for 1997]
DROP VIEW IF EXISTS "Product Sales for 1997";
CREATE VIEW "Product Sales for 1997" AS
    SELECT
        Categories.CategoryName,
        Products.ProductName,
        SUM(("Order Details".UnitPrice*Quantity*(1-Discount)/100)*100) AS ProductSales
    FROM Categories
    JOIN Products On Categories.CategoryID = Products.CategoryID
    JOIN "Order Details" on Products.ProductID = "Order Details".ProductID
    JOIN Orders on Orders.OrderID = "Order Details".OrderID
    WHERE Orders.ShippedDate Between DATE '1997-01-01' And DATE '1997-12-31'
    GROUP BY Categories.CategoryName, Products.ProductName;

-- VIEW [Products Above Average Price]
DROP VIEW IF EXISTS "Products Above Average Price";
CREATE VIEW "Products Above Average Price" AS
    SELECT Products.ProductName, Products.UnitPrice
    FROM Products
    WHERE Products.UnitPrice > (SELECT AVG(UnitPrice) From Products);

-- VIEW [Products by Category]
DROP VIEW IF EXISTS "Products by Category";
CREATE VIEW "Products by Category" AS
    SELECT
        Categories.CategoryName,
        Products.ProductName,
        Products.QuantityPerUnit,
        Products.UnitsInStock,
        Products.Discontinued
    FROM Categories
    INNER JOIN Products ON Categories.CategoryID = Products.CategoryID
    WHERE Products.Discontinued <> 1;

-- VIEW [Quarterly Orders]
DROP VIEW IF EXISTS "Quarterly Orders";
CREATE VIEW "Quarterly Orders" AS
    SELECT DISTINCT
        Customers.CustomerID,
        Customers.CompanyName,
        Customers.City,
        Customers.Country
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    WHERE Orders.OrderDate BETWEEN DATE '1997-01-01' And DATE '1997-12-31';

-- VIEW [Sales Totals by Amount]
DROP VIEW IF EXISTS "Sales Totals by Amount";
CREATE VIEW "Sales Totals by Amount" AS
    SELECT
        "Order Subtotals".Subtotal AS SaleAmount,
        Orders.OrderID,
        Customers.CompanyName,
        Orders.ShippedDate
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    JOIN "Order Subtotals" ON Orders.OrderID = "Order Subtotals".OrderID
    WHERE
        ("Order Subtotals".Subtotal >2500)
        AND (Orders.ShippedDate BETWEEN DATE '1997-01-01' And DATE '1997-12-31');

-- VIEW [Summary of Sales by Quarter]
DROP VIEW IF EXISTS "Summary of Sales by Quarter";
CREATE VIEW "Summary of Sales by Quarter" AS
    SELECT
        Orders.ShippedDate,
        Orders.OrderID,
        "Order Subtotals".Subtotal
    FROM Orders
    INNER JOIN "Order Subtotals" ON Orders.OrderID = "Order Subtotals".OrderID
    WHERE Orders.ShippedDate IS NOT NULL;

-- VIEW [Summary of Sales by Year]
DROP VIEW IF EXISTS "Summary of Sales by Year";
CREATE VIEW "Summary of Sales by Year" AS
    SELECT Orders.ShippedDate, Orders.OrderID, "Order Subtotals".Subtotal
    FROM Orders
    INNER JOIN "Order Subtotals" ON Orders.OrderID = "Order Subtotals".OrderID
    WHERE Orders.ShippedDate IS NOT NULL;

-- VIEW [Category Sales for 1997]
DROP VIEW IF EXISTS "Category Sales for 1997";
CREATE VIEW "Category Sales for 1997" AS
    SELECT
        "Product Sales for 1997".CategoryName,
        SUM("Product Sales for 1997".ProductSales) AS CategorySales
    FROM "Product Sales for 1997"
    GROUP BY "Product Sales for 1997".CategoryName;

-- VIEW [Order Details Extended]
DROP VIEW IF EXISTS "Order Details Extended";
CREATE VIEW "Order Details Extended" AS
    SELECT
        "Order Details".OrderID,
        "Order Details".ProductID,
        Products.ProductName,
        "Order Details".UnitPrice,
        "Order Details".Quantity,
        "Order Details".Discount,
        ("Order Details".UnitPrice*Quantity*(1-Discount)/100)*100 AS ExtendedPrice
    FROM Products
    JOIN "Order Details" ON Products.ProductID = "Order Details".ProductID;

-- VIEW [Sales by Category]
DROP VIEW IF EXISTS "Sales by Category";
CREATE VIEW "Sales by Category" AS
    SELECT
        Categories.CategoryID,
        Categories.CategoryName,
        Products.ProductName,
        SUM("Order Details Extended".ExtendedPrice) AS ProductSales
    FROM Categories
    JOIN Products ON Categories.CategoryID = Products.CategoryID
    JOIN "Order Details Extended" ON Products.ProductID = "Order Details Extended".ProductID
    JOIN Orders ON Orders.OrderID = "Order Details Extended".OrderID
    WHERE Orders.OrderDate BETWEEN DATE '1997-01-01' And DATE '1997-12-31'
    GROUP BY Categories.CategoryID, Categories.CategoryName, Products.ProductName;
