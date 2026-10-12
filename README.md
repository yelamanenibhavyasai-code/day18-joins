# Day 18 — Spark SQL Joins

## Project Overview

This project demonstrates Spark SQL joins using Scala DataFrames. It combines orders, customers, and payments to generate a complete order report and identify orders without matching payments.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.3
- Java 17
- SBT 1.12.15
- Ubuntu on WSL2

## Concepts Covered

- Inner Join
- Left Join
- Right Join
- Full Outer Join
- DataFrame aliases and ambiguous column handling
- NULL handling using `coalesce()` and `isNull`
- Joining three DataFrames
- Spark physical execution plans

## Scenarios Implemented

### 1. Orders and Customers

Combines order information with customer names and cities.

### 2. Payment Status Analysis

Joins orders with payment records to identify paid, pending, and unpaid orders.

### 3. Complete Order Report

Combines orders, customers, and payments into a single report.

### 4. Orders Without Payments

Identifies orders that do not have a matching payment record.

### 5. Join Execution Plan

Uses `explain("formatted")` to inspect the physical execution plan selected by Spark.

## Sample Results

| Order ID | Customer | Order Amount | Payment Status |
|---|---|---:|---|
| 101 | Ravi | 2500 | Paid |
| 102 | Priya | 4000 | Paid |
| 103 | Ravi | 1500 | No Payment |
| 104 | Unknown | 3000 | Pending |
| 105 | Arjun | 5000 | No Payment |

*All results are based on sample data created in the program.*

## How to Run

Compile:

```bash
sbt compile
```

Run:

```bash
sbt "runMain Day18Joins"
```

## Key Learnings

- Combining multiple DataFrames using joins.
- Handling unmatched records and NULL values.
- Resolving ambiguous column names using aliases.
- Inspecting Spark's physical join strategy.

## Author

**Yelamaneni Bhavyasai**

30 Days Scala and Apache Spark Training — Day 18.
