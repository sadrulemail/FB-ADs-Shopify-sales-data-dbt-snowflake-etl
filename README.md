**E-Commerce Sales & Advertising Analytics Warehouse**
 
**Overview**

This repository hosts the Data Build Tool (dbt) project responsible for transforming raw e-commerce and advertising data into a unified, clean, and analytical data warehouse. The goal is to combine operational metrics (from Shopify) with marketing performance metrics (from Facebook Ads) to accurately calculate key business metrics like Customer Acquisition Cost (CAC), Return on Ad Spend (ROAS), and Lifetime Value (LTV).

**Architecture Flow**
Raw data is ingested via an ETL/ELT process into a cloud data warehouse (e.g., Snowflake, BigQuery) from AWS S3, where DBT models structure and enhance the data for consumption by BI tools.


📁 Data Sources
The warehouse is built upon two primary data streams, which are assumed to be loaded into your staging schema (e.g., raw or staging) via an S3 integration process.

Source System

Core Tables (Raw Input)

Description

Shopify

raw_orders, raw_customers, raw_order_items, raw_products

E-commerce transactional and customer data.

Facebook Ads

raw_fb_campaigns, raw_fb_adsets, raw_fb_ads, raw_fb_ad_insights

Marketing hierarchy and daily performance metrics (Spend, Clicks, Impressions).

📦 Core Data Model (Marts)
The DBT project generates the following key dimensional and fact tables for downstream analytics:

dim_customers
A refined dimension table for customer information, including calculated lifetime metrics.

dim_products
A clean dimension table for product details.

fct_ecommerce_orders
A fact table for all Shopify orders, including calculated profit margins.

fct_ads_performance (Marketing Mart)
Aggregated daily performance data from Facebook Ads, ready for pure marketing analysis (CPC, CPM, CTR).

agg_sales_ads_daily (Unified Fact Table)
The central table for Sales & Advertising Analytics. This fact table combines daily ad spend (from Facebook Ads) with daily conversion value (from Shopify Orders) based on the order date, enabling direct ROAS calculation.

Table Name

Key Columns (Comma Separated)

dim_customers

customer_id, first_order_date, total_ltv

dim_products

product_id, product_title, vendor

fct_ecommerce_orders

order_id, customer_id, order_date, total_revenue, total_profit

fct_ads_performance

date, ad_id, campaign_id, spend, impressions, clicks

agg_sales_ads_daily

date, campaign_id, adset_id, total_spend, total_conversions, total_attributed_revenue

⚙️ Project Structure (DBT Models)
The models directory is organized into layers to reflect the transformation stages:

Directory

Type

Key Models (Example File Names)

Description

models/staging

Staging

stg_ads.sql, stg_campaigns.sql, stg_ad_insights.sql

Direct transformation of raw source tables. Simple cleaning, casting, and renaming. These are the foundation.

models/dimensions

Dimensions

dim_customer.sql, dim_product.sql, dim_date.sql

Core dimension tables for descriptive attributes, optimized for joining.

models/facts

Facts

fact_sales.sql, fact_ad_performance.sql, fact_order_summary.sql

Granular fact tables capturing business events and metrics.

models/analytics

Marts/Analytics

ad_effectiveness.sql, campaign_performance.sql, customer_segmentation.sql

Final aggregated models (Data Marts) ready for consumption by BI tools, calculating key business logic (ROAS, CAC).

macros

Macros

generate_date_dimension.sql

Reusable SQL snippets and functions used across the project (e.g., for generating date tables).

🛠️ Getting Started
Prerequisites
You need the following installed and configured:

DBT CLI: Installation of the Data Build Tool.

Data Warehouse Access: Credentials configured for your target warehouse (e.g., profiles.yml for Snowflake, BigQuery, or Redshift).

AWS S3 Access: Permissions to the S3 bucket where the raw CSV files are stored.

Installation & Setup
Clone the repository:

git clone git@github.com:your-company/sales-ads-warehouse.git
cd sales-ads-warehouse



Install dependencies:

dbt deps



Check Connection:

dbt debug



Running the Project
The following commands will execute the full transformation pipeline:

Run all models:

dbt run



Run specific marts (e.g., the unified table):

dbt run --select agg_sales_ads_daily



Test data quality:
(Recommended to run after every build to ensure referential integrity and data constraints.)

dbt test



🚀 Further Development
Future enhancements could include:

Attribution Logic: Implement multi-touch attribution (e.g., first-click, linear) within the agg_sales_ads_daily model instead of simple last-click logic.

Google Ads Integration: Add a new source to the fct_ads_performance layer to include Google Search and Shopping campaign data.

CI/CD Pipeline: Implement GitHub Actions (or equivalent CI/CD tool) to automate dbt test and dbt run upon code merge to the main branch, ensuring quality and automated deployment.

Dimension SCD: Implement Slowly Changing Dimensions (SCD Type 2) for tracking historical changes in product prices or campaign budgets.
