# End-to-End-Ride-Streaming-PySpark-dbt-Project
End-to-end Ride Sharing Data Engineering project using PySpark, Delta Lake, Databricks Structured Streaming, dbt, SCD Type 2 Snapshots, and Dimensional Modeling.
# End-to-End Ride Streaming PySpark dbt Project

## Overview

This project demonstrates the implementation of a modern Lakehouse architecture for a ride-sharing platform using Databricks, PySpark, Delta Lake, Structured Streaming, and dbt.

The solution processes ride-sharing data through Bronze, Silver, and Gold layers, implements Slowly Changing Dimensions (SCD Type 2) using dbt snapshots, and delivers business-ready analytical views for reporting and decision-making.

---

## Architecture

[![Architecture](architecture/architecture.png)](https://github.com/AyeshaShaik26/End-to-End-Ride-Streaming-PySpark-dbt-Project/blob/main/Architecture/architecture_diagram.png)

---

## Technology Stack

* Databricks
* PySpark
* Delta Lake
* Structured Streaming
* dbt
* Databricks SQL Warehouse
* Unity Catalog
* SQL
* GitHub

---

## Key Features

* Structured Streaming Data Processing
* Medallion Architecture (Bronze, Silver, Gold)
* Incremental Data Processing
* PySpark Transformations
* dbt Models
* dbt Snapshots (SCD Type 2)
* Dimensional Data Modeling
* Jinja-based Dynamic SQL Generation
* Business KPI Views

---

## Data Model

### Fact Table

* FactTrips

### Dimension Tables

* DimCustomers
* DimDrivers
* DimVehicles
* DimLocations
* DimPayments

---

## SCD Type 2 Implementation

Implemented using dbt Snapshots for:

* Customers
* Drivers
* Vehicles
* Locations
* Payments

Historical changes are tracked using:

* dbt_scd_id
* dbt_valid_from
* dbt_valid_to

---

## Gold Layer KPI Views

### Revenue Performance

* Total Revenue
* Average Fare
* Total Trips
* Revenue Trends

### Driver Performance

* Trips Completed
* Revenue Generated
* Driver Ratings
* Average Distance Travelled

### Customer Lifetime Value

* Total Trips per Customer
* Lifetime Revenue
* Average Fare
* Last Trip Date

### Payment Analytics

* Revenue by Payment Method
* Transaction Volume
* Payment Status Analysis

---

## Project Structure

```text
End-to-End-Ride-Streaming-PySpark-dbt-Project
│
├── architecture
├── databricks
│   ├── bronze
│   ├── silver
│   └── streaming
├── dbt
│   ├── models
│   ├── snapshots
│   ├── tests
│   └── macros
├── screenshots
└── README.md
```

---

## Skills Demonstrated

* Data Engineering
* Analytics Engineering
* PySpark
* Databricks
* Delta Lake
* Structured Streaming
* dbt
* Jinja
* SCD Type 2
* Dimensional Modeling
* SQL Optimization
* Data Quality and Governance

---

## Future Enhancements

* CI/CD Integration
* Data Quality Monitoring
* Automated Testing Framework
* Power BI Dashboard Integration
* Real-Time Business Alerts
