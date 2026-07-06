# Enterprise Operations Intelligence Platform

> A modern analytics engineering project demonstrating how raw operational data can be transformed into trusted, analytics-ready datasets using Python, PostgreSQL, dbt, and dimensional modeling.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

---

# Overview

Organizations rely on accurate, well-modeled data to make strategic decisions. However, operational data is often fragmented, inconsistent, and difficult to analyze directly.

This project demonstrates a complete analytics engineering workflow that transforms raw business data into a production-style dimensional model optimized for reporting and business intelligence.

The pipeline includes data cleaning, validation, database ingestion, dimensional modeling with dbt, and dashboard-ready datasets suitable for executive reporting.

---

# Business Problem

Business teams often face challenges such as:

- Duplicate customer records
- Missing or inconsistent values
- Difficult-to-query transactional datasets
- Lack of standardized business metrics
- Slow reporting caused by poorly structured data

This project addresses those problems by building a repeatable analytics pipeline that converts raw operational data into trusted business datasets.

---

# Architecture

```
                 Raw CSV Files
                       │
                       ▼
            Python Cleaning Pipeline
                       │
                       ▼
          Validation & Data Quality Checks
                       │
                       ▼
             PostgreSQL Raw Schema
                       │
                       ▼
                 dbt Staging Models
                       │
                       ▼
              Dimension & Fact Models
                       │
                       ▼
              Analytics-Ready Data Marts
                       │
                       ▼
              Power BI Dashboards
```

---

# Project Goals

- Build an end-to-end analytics engineering pipeline
- Apply modern ELT principles
- Create a scalable dimensional model
- Improve data quality through automated validation
- Produce analytics-ready datasets for business reporting
- Demonstrate production-style analytics engineering workflows

---

# Technology Stack

## Languages

- Python
- SQL

## Data Engineering

- PostgreSQL
- dbt
- CSV Processing

## Analytics

- Power BI
- Star Schema Modeling
- Data Validation
- Data Quality Testing

## Development

- Git
- Virtual Environments
- Environment Variables

---

# Pipeline Overview

## 1. Data Ingestion

Raw business data is collected from multiple CSV files representing:

- Customers
- Products
- Stores
- Regions
- Orders

---

## 2. Data Cleaning

Python scripts standardize and clean incoming datasets by:

- Removing duplicates
- Handling missing values
- Standardizing identifiers
- Normalizing formats
- Preparing records for loading

---

## 3. Data Validation

Before loading, datasets undergo quality checks including:

- Schema validation
- Null checks
- Duplicate detection
- Data type validation
- Foreign key validation
- Referential integrity

This ensures only trusted data enters the warehouse.

---

## 4. Database Loading

Validated datasets are loaded into PostgreSQL using a dedicated raw schema.

Separating raw data from transformed models preserves source integrity while supporting reproducible transformations.

---

## 5. Analytics Engineering with dbt

dbt transforms raw operational tables into analytics-ready models using a layered architecture.

### Staging Models

- Clean source tables
- Standardize naming conventions
- Prepare datasets for business logic

### Dimension Models

- Customers
- Products
- Stores
- Regions
- Dates

### Fact Models

- Orders
- Order Line Items

This dimensional design enables efficient reporting while reducing query complexity.

---

# Star Schema

The final warehouse follows a dimensional star schema.

```
                Dim Customer
                      │
                      │
Dim Product ─── Fact Orders ─── Dim Store
                      │
                      │
                 Dim Date
                      │
                      │
                 Dim Region
```

This design supports fast aggregation, simplified reporting, and scalable analytics.

---

# Dashboard Layer

The transformed data is designed for business intelligence tools such as Power BI.

Example reporting includes:

- Executive KPI dashboards
- Revenue analysis
- Store performance
- Regional trends
- Product performance
- Customer insights

Example dashboard exports are included in the repository.

---

# Repository Structure

```
Enterprise_BI_Project_Repo/

├── python/
│   ├── cleaning
│   ├── validation
│   ├── loading
│   └── pipeline
│
├── models/
│   ├── sources
│   ├── staging
│   └── marts
│
├── dashboard_data/
│
├── dashboards/
│
├── docs/
│
├── data/
│
└── README.md
```

---

# Key Skills Demonstrated

- Analytics Engineering
- Data Modeling
- ETL / ELT Pipelines
- Data Quality
- SQL Development
- Python Automation
- PostgreSQL
- dbt
- Dimensional Modeling
- Business Intelligence
- Dashboard Development

---

# Lessons Learned

This project strengthened my understanding of modern analytics engineering practices, including:

- Designing scalable dimensional models
- Building maintainable ELT pipelines
- Separating raw and transformed datasets
- Automating data validation
- Creating reusable analytics models
- Structuring data for business decision-making

---

# Future Improvements

Potential enhancements include:

- Dockerized deployment
- CI/CD pipeline
- Automated scheduling
- Incremental dbt models
- Cloud warehouse migration
- Great Expectations data quality tests
- Data lineage visualization
- Automated monitoring and alerts

---

# About Me

I'm passionate about building products and analytics systems that combine data engineering, experimentation, and AI to help organizations make better decisions.

I'm currently focused on:

- Product Analytics
- Experimentation
- Analytics Engineering
- AI-powered Products
- Business Intelligence

Feel free to connect or reach out if you'd like to discuss analytics engineering, product analytics, or AI.

---

## License

This project is intended for educational and portfolio purposes.
