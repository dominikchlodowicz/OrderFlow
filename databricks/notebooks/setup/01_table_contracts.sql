%sql

-- Primary-key and foreign-key constraints are informational in Databricks.
-- UNIQUE constraints from the DBML contracts are intentionally omitted because
-- they are not supported by Databricks Runtime 17.3; their definitions remain
-- beside the corresponding tables as non-executable comments.

-- Bronze contracts: raw source fields remain nullable STRING columns; only the
-- lineage fields defined by bronze_lineage are typed and required.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.calendar (
  date_day STRING,
  year STRING,
  quarter STRING,
  month STRING,
  day_of_month STRING,
  day_of_week STRING,
  day_name STRING,
  week_of_year STRING,
  is_weekend STRING,
  is_polish_public_holiday STRING,
  holiday_name STRING,
  load_date STRING,
  loaded_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.customers (
  customer_id STRING,
  email STRING,
  first_name STRING,
  last_name STRING,
  country_code STRING,
  city STRING,
  created_at STRING,
  updated_at STRING,
  customer_status STRING,
  marketing_consent STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.marketing_campaigns (
  campaign_id STRING,
  campaign_name STRING,
  source_channel STRING,
  start_date STRING,
  end_date STRING,
  budget_amount STRING,
  currency STRING,
  created_at STRING,
  updated_at STRING,
  is_active STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.order_items (
  order_item_id STRING,
  order_id STRING,
  product_id STRING,
  quantity STRING,
  unit_price STRING,
  discount_amount STRING,
  line_total STRING,
  created_at STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.orders (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_created_at STRING,
  order_updated_at STRING,
  country_code STRING,
  currency STRING,
  gross_amount STRING,
  discount_amount STRING,
  net_amount STRING,
  source_channel STRING,
  campaign_id STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.payments (
  payment_id STRING,
  order_id STRING,
  payment_attempt_number STRING,
  payment_method STRING,
  payment_status STRING,
  amount STRING,
  currency STRING,
  created_at STRING,
  processed_at STRING,
  failure_reason STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.products (
  product_id STRING,
  sku STRING,
  product_name STRING,
  category STRING,
  brand STRING,
  unit_price STRING,
  currency STRING,
  is_active STRING,
  created_at STRING,
  updated_at STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.refunds (
  refund_id STRING,
  order_id STRING,
  payment_id STRING,
  refund_reason STRING,
  refund_amount STRING,
  currency STRING,
  created_at STRING,
  processed_at STRING,
  refund_status STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.shipments (
  shipment_id STRING,
  order_id STRING,
  carrier STRING,
  shipment_status STRING,
  shipped_at STRING,
  estimated_delivery_at STRING,
  delivered_at STRING,
  delivery_country STRING,
  delivery_city STRING,
  shipping_cost STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.web_events (
  event_id STRING,
  session_id STRING,
  customer_id STRING,
  anonymous_id STRING,
  event_type STRING,
  event_timestamp STRING,
  product_id STRING,
  campaign_id STRING,
  device_type STRING,
  country_code STRING,
  page_url STRING,
  load_date STRING,
  loaded_at STRING,
  source_event_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.exchange_rates (
  rate_date STRING,
  currency STRING,
  rate_to_pln STRING,
  source STRING,
  load_date STRING,
  loaded_at STRING,
  _source_system STRING NOT NULL COMMENT 'System that produced the source data',
  _source_entity STRING NOT NULL COMMENT 'Source dataset or entity name',
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file',
  _source_load_date DATE NOT NULL COMMENT 'Delivery or partition date assigned by the ingestion convention',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier shared by all rows processed in the same Bronze ingestion run',
  _ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 hash of the ordered raw source fields, excluding ingestion metadata'
)
USING DELTA;

-- COMMAND ----------

-- Silver contracts: conformed types and required fields from silver.dbml.

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.calendar (
  date_day DATE,
  year INT,
  quarter INT,
  month INT,
  day_of_month INT,
  day_of_week INT,
  day_name STRING,
  week_of_year INT,
  is_weekend BOOLEAN,
  is_polish_public_holiday BOOLEAN,
  holiday_name STRING,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.customers (
  customer_id STRING NOT NULL COMMENT 'Source customer identifier',
  email STRING NOT NULL,
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  country_code STRING NOT NULL COMMENT 'Uppercase ISO 3166-1 alpha-2 country code',
  city STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP COMMENT 'Must not precede created_at',
  customer_status STRING NOT NULL COMMENT 'One of: active, inactive',
  marketing_consent BOOLEAN NOT NULL,
  load_date DATE,
  loaded_at TIMESTAMP,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (customer_id)
  -- DBML uniqueness (documentation only): UNIQUE (email)
  PRIMARY KEY (customer_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.order_items (
  order_item_id STRING NOT NULL,
  order_id STRING NOT NULL,
  product_id STRING NOT NULL,
  quantity INT NOT NULL COMMENT 'Must be greater than 0',
  unit_price DECIMAL(18, 2) NOT NULL COMMENT 'Must be greater than or equal to 0',
  discount_amount DECIMAL(18, 2) NOT NULL COMMENT 'Null standardized to 0',
  gross_amount DECIMAL(18, 2) NOT NULL COMMENT 'quantity * unit_price',
  line_total DECIMAL(18, 2) NOT NULL COMMENT 'gross_amount - discount_amount',
  created_at TIMESTAMP,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (order_item_id)
  PRIMARY KEY (order_item_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.orders (
  order_id STRING NOT NULL,
  customer_id STRING,
  order_status STRING NOT NULL COMMENT 'One of: created, paid, shipped, cancelled, returned',
  order_created_at TIMESTAMP NOT NULL,
  order_updated_at TIMESTAMP COMMENT 'Must not precede order_created_at',
  country_code STRING NOT NULL COMMENT 'Uppercase ISO 3166-1 alpha-2 country code',
  currency STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  gross_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  discount_amount DECIMAL(18, 2) NOT NULL COMMENT 'Null standardized to 0; must be between 0 and gross_amount',
  net_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must equal gross_amount - discount_amount',
  source_channel STRING NOT NULL,
  campaign_id STRING,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (order_id)
  PRIMARY KEY (order_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.payments (
  payment_id STRING NOT NULL COMMENT 'Source payment identifier',
  order_id STRING NOT NULL COMMENT 'Source order identifier',
  payment_attempt_number INT NOT NULL,
  payment_method STRING NOT NULL COMMENT 'One of: card, paypal, blik, bank_transfer, on delivery, online installments',
  payment_status STRING NOT NULL COMMENT 'One of: captured, authorized, failed',
  failure_reason STRING COMMENT 'Required when payment_status = failed. One of: timeout, insufficient_funds, card_declined. Otherwise null.',
  amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  currency STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  created_at TIMESTAMP NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  load_date DATE,
  loaded_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (payment_id)
  PRIMARY KEY (payment_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.web_events (
  event_id STRING NOT NULL COMMENT 'Source web event identifier',
  session_id STRING NOT NULL,
  customer_id STRING,
  anonymous_id STRING NOT NULL,
  event_type STRING NOT NULL COMMENT 'TODO: add one-of rule after Bronze ingestion to identify the event types in the source data',
  event_timestamp TIMESTAMP,
  product_id STRING COMMENT 'Required only for product-related event types',
  campaign_id STRING,
  device_type STRING NOT NULL COMMENT 'One of: tablet, mobile, desktop',
  country_code STRING NOT NULL COMMENT 'Uppercase ISO 3166-1 alpha-2 country code',
  page_url STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL,
  source_event_at TIMESTAMP NOT NULL,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (event_id)
  PRIMARY KEY (event_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.shipments (
  shipment_id STRING NOT NULL COMMENT 'Source shipment identifier',
  order_id STRING NOT NULL,
  carrier STRING NOT NULL,
  shipment_status STRING NOT NULL COMMENT 'One of: lost, shipped, delivered',
  shipped_at TIMESTAMP NOT NULL,
  estimated_delivery_at DATE COMMENT 'estimated_delivery_at >= DATE(shipped_at)',
  delivered_at TIMESTAMP COMMENT 'If shipment_status = delivered, must not be null and must be >= shipped_at',
  delivery_country STRING NOT NULL,
  delivery_city STRING NOT NULL,
  shipping_cost DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  load_date DATE,
  loaded_at TIMESTAMP,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (shipment_id)
  PRIMARY KEY (shipment_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.refunds (
  refund_id STRING NOT NULL COMMENT 'Source refund identifier',
  order_id STRING NOT NULL,
  payment_id STRING NOT NULL,
  refund_reason STRING,
  refund_status STRING NOT NULL COMMENT 'TODO: add one-of rule after Bronze ingestion to identify the refund statuses in the source data',
  refund_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  currency STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  created_at TIMESTAMP NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  load_date DATE,
  loaded_at TIMESTAMP,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (refund_id)
  PRIMARY KEY (refund_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.marketing_campaigns (
  campaign_id STRING NOT NULL COMMENT 'Source campaign identifier',
  campaign_name STRING NOT NULL,
  source_channel STRING,
  start_date DATE NOT NULL,
  end_date DATE,
  budget_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be > 0',
  currency STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  is_active BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  load_date DATE,
  loaded_at TIMESTAMP,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (campaign_id)
  PRIMARY KEY (campaign_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.products (
  product_id STRING NOT NULL COMMENT 'Source product identifier',
  sku STRING NOT NULL,
  product_name STRING NOT NULL,
  category STRING,
  brand STRING,
  unit_price DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  currency STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  is_active BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  load_date DATE,
  loaded_at TIMESTAMP,
  source_event_at TIMESTAMP,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver',
  -- DBML uniqueness (documentation only): UNIQUE (product_id)
  PRIMARY KEY (product_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.exchange_rates (
  rate_date DATE,
  currency STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  rate_to_pln DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0.0',
  source STRING,
  load_date DATE NOT NULL,
  loaded_at TIMESTAMP NOT NULL,
  _source_file_name STRING NOT NULL COMMENT 'Name of the physical source file containing the winning Bronze record',
  _source_file_path STRING NOT NULL COMMENT 'Full physical path of the source file containing the winning Bronze record',
  _ingestion_run_id STRING NOT NULL COMMENT 'Identifier of the Bronze ingestion run that introduced the winning record',
  _bronze_ingested_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the winning record was written to Bronze',
  _raw_record_hash STRING NOT NULL COMMENT 'SHA-256 identifier of the exact raw source version selected for Silver',
  _silver_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced in Silver'
)
USING DELTA;

-- COMMAND ----------

-- Gold dimensions are created before facts so every DBML reference resolves to
-- an existing Unity Catalog primary key.

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.dim_calendar (
  date_key INT NOT NULL COMMENT 'Surrogate key in YYYYMMDD format',
  date_day DATE NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL COMMENT 'Integer from 1 to 4',
  month INT NOT NULL COMMENT 'Integer from 1 to 12',
  month_name STRING NOT NULL,
  day_of_month INT NOT NULL,
  day_of_week INT NOT NULL COMMENT 'Use one documented numbering convention consistently',
  day_name STRING NOT NULL,
  week_of_year INT NOT NULL,
  is_weekend BOOLEAN NOT NULL,
  is_polish_public_holiday BOOLEAN NOT NULL,
  holiday_name STRING,
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (date_day)
  PRIMARY KEY (date_key)
)
USING DELTA
COMMENT 'Grain: one calendar date';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.dim_customers (
  customer_key BIGINT NOT NULL,
  customer_id STRING NOT NULL COMMENT 'Source customer identifier',
  email STRING,
  email_domain STRING,
  first_name STRING,
  last_name STRING,
  full_name STRING,
  country_code STRING NOT NULL COMMENT 'Uppercase ISO 3166-1 alpha-2 country code',
  city STRING,
  customer_status STRING,
  is_active_customer BOOLEAN NOT NULL,
  marketing_consent BOOLEAN NOT NULL,
  registered_at TIMESTAMP,
  registration_date DATE COMMENT 'Date component of registered_at; stored as an attribute to keep dimensions denormalized',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (customer_id)
  PRIMARY KEY (customer_key)
)
USING DELTA
COMMENT 'Grain: one current customer per customer_id (SCD Type 1)';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.dim_products (
  product_key BIGINT NOT NULL,
  product_id STRING NOT NULL COMMENT 'Source product identifier',
  sku STRING NOT NULL,
  product_name STRING NOT NULL,
  category STRING,
  brand STRING,
  unit_price DECIMAL(18, 2) NOT NULL COMMENT 'Current list price; must be >= 0. Transaction prices remain in fct_order_items',
  currency_code STRING NOT NULL COMMENT 'Currency of the current list price; uppercase ISO 4217 code',
  is_active BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP COMMENT 'Must not precede created_at',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (product_id)
  -- DBML uniqueness (documentation only): UNIQUE (sku)
  PRIMARY KEY (product_key)
)
USING DELTA
COMMENT 'Grain: one current product per product_id (SCD Type 1)';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.dim_campaigns (
  campaign_key BIGINT NOT NULL,
  campaign_id STRING NOT NULL COMMENT 'Source campaign identifier',
  campaign_name STRING NOT NULL,
  source_channel STRING COMMENT 'Standardized channel; accepted domain defined after profiling Bronze',
  start_date DATE NOT NULL,
  end_date DATE COMMENT 'Nullable for open-ended campaigns; must not precede start_date',
  budget_amount DECIMAL(18, 2) NOT NULL COMMENT 'Planned campaign budget; must be > 0',
  budget_currency_code STRING NOT NULL COMMENT 'Currency of budget_amount; uppercase ISO 4217 code',
  is_active BOOLEAN NOT NULL COMMENT 'Source-provided operational status',
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP COMMENT 'Must not precede created_at',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (campaign_id)
  PRIMARY KEY (campaign_key)
)
USING DELTA
COMMENT 'Grain: one current campaign per campaign_id (SCD Type 1)';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.dim_currency (
  currency_key BIGINT NOT NULL,
  currency_code STRING NOT NULL COMMENT 'Uppercase ISO 4217 currency code',
  is_reporting_currency BOOLEAN NOT NULL COMMENT 'True for PLN',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (currency_code)
  PRIMARY KEY (currency_key)
)
USING DELTA
COMMENT 'Grain: one ISO 4217 currency';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_orders (
  order_key BIGINT NOT NULL,
  order_id STRING NOT NULL COMMENT 'Source order identifier retained as a degenerate dimension',
  customer_key BIGINT NOT NULL COMMENT 'Use the Guest or Unknown member when source customer_id is null or unresolved',
  campaign_key BIGINT NOT NULL COMMENT 'Use the No campaign or Unknown member when source campaign_id is null or unresolved',
  currency_key BIGINT NOT NULL,
  order_date_key INT NOT NULL,
  order_created_at TIMESTAMP NOT NULL,
  order_updated_at TIMESTAMP COMMENT 'Must not precede order_created_at',
  order_status STRING NOT NULL COMMENT 'One of: created, paid, shipped, cancelled, returned',
  order_country_code STRING NOT NULL COMMENT 'Country recorded on the order; uppercase ISO 3166-1 alpha-2 code',
  source_channel STRING NOT NULL,
  gross_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  discount_amount DECIMAL(18, 2) NOT NULL COMMENT 'Null standardized to 0; must be between 0 and gross_amount',
  net_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must equal gross_amount - discount_amount',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (order_id)
  PRIMARY KEY (order_key),
  FOREIGN KEY (customer_key) REFERENCES orderflow_dev.gold.dim_customers (customer_key),
  FOREIGN KEY (campaign_key) REFERENCES orderflow_dev.gold.dim_campaigns (campaign_key),
  FOREIGN KEY (currency_key) REFERENCES orderflow_dev.gold.dim_currency (currency_key),
  FOREIGN KEY (order_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key)
)
USING DELTA
COMMENT 'Grain: one latest accepted order per order_id. Current-state/accumulating-snapshot fact.';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_order_items (
  order_item_key BIGINT NOT NULL,
  order_item_id STRING NOT NULL COMMENT 'Source order-item identifier',
  order_id STRING NOT NULL COMMENT 'Source order identifier retained as a degenerate dimension; not a foreign key to fct_orders',
  customer_key BIGINT NOT NULL COMMENT 'Use the same Guest or Unknown member selected for the order',
  product_key BIGINT NOT NULL,
  campaign_key BIGINT NOT NULL COMMENT 'Use the same No campaign or Unknown member selected for the order',
  currency_key BIGINT NOT NULL COMMENT 'Order currency inherited from the accepted order header',
  order_date_key INT NOT NULL,
  order_item_created_date_key INT NOT NULL,
  order_country_code STRING NOT NULL COMMENT 'Country inherited from the accepted order header',
  order_item_created_at TIMESTAMP NOT NULL,
  quantity INT NOT NULL COMMENT 'Must be > 0',
  unit_price DECIMAL(18, 2) NOT NULL COMMENT 'Transaction price; must be >= 0',
  discount_amount DECIMAL(18, 2) NOT NULL COMMENT 'Standardized to 0 when source is null',
  gross_amount DECIMAL(18, 2) NOT NULL COMMENT 'quantity * unit_price',
  line_total DECIMAL(18, 2) NOT NULL COMMENT 'gross_amount - discount_amount',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (order_item_id)
  CONSTRAINT pk_fct_order_items PRIMARY KEY (order_item_key),
  CONSTRAINT fk_fct_order_items_customer FOREIGN KEY (customer_key) REFERENCES orderflow_dev.gold.dim_customers (customer_key),
  CONSTRAINT fk_fct_order_items_product FOREIGN KEY (product_key) REFERENCES orderflow_dev.gold.dim_products (product_key),
  CONSTRAINT fk_fct_order_items_campaign FOREIGN KEY (campaign_key) REFERENCES orderflow_dev.gold.dim_campaigns (campaign_key),
  CONSTRAINT fk_fct_order_items_currency FOREIGN KEY (currency_key) REFERENCES orderflow_dev.gold.dim_currency (currency_key),
  CONSTRAINT fk_fct_order_items_order_date FOREIGN KEY (order_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key),
  CONSTRAINT fk_fct_order_items_created_date FOREIGN KEY (order_item_created_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key)
)
USING DELTA
COMMENT 'Grain: one accepted order item per order_item_id. Customer, campaign, currency, country, and order-date context is enriched from the accepted order header.';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_payments (
  payment_key BIGINT NOT NULL,
  payment_id STRING NOT NULL COMMENT 'Source payment identifier',
  order_id STRING NOT NULL COMMENT 'Source order identifier retained as a degenerate dimension; not a foreign key to fct_orders',
  customer_key BIGINT NOT NULL COMMENT 'Enriched from the accepted order header',
  campaign_key BIGINT NOT NULL COMMENT 'Enriched from the accepted order header',
  currency_key BIGINT NOT NULL,
  payment_created_date_key INT NOT NULL,
  payment_processed_date_key INT NOT NULL,
  payment_attempt_number INT NOT NULL COMMENT 'Must be > 0 within an order',
  payment_method STRING NOT NULL COMMENT 'One of: card, paypal, blik, bank_transfer, cash_on_delivery, online_installments',
  payment_status STRING NOT NULL COMMENT 'One of: authorized, captured, failed',
  failure_reason STRING COMMENT 'Required when payment_status = failed; otherwise null. One of: timeout, insufficient_funds, card_declined',
  amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  created_at TIMESTAMP NOT NULL,
  processed_at TIMESTAMP NOT NULL COMMENT 'Must not precede created_at',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (payment_id)
  PRIMARY KEY (payment_key),
  FOREIGN KEY (customer_key) REFERENCES orderflow_dev.gold.dim_customers (customer_key),
  FOREIGN KEY (campaign_key) REFERENCES orderflow_dev.gold.dim_campaigns (campaign_key),
  FOREIGN KEY (currency_key) REFERENCES orderflow_dev.gold.dim_currency (currency_key),
  CONSTRAINT fk_fct_payments_created_date FOREIGN KEY (payment_created_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key),
  CONSTRAINT fk_fct_payments_processed_date FOREIGN KEY (payment_processed_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key)
)
USING DELTA
COMMENT 'Grain: one latest accepted payment per payment_id. Current-state payment fact.';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_refunds (
  refund_key BIGINT NOT NULL,
  refund_id STRING NOT NULL COMMENT 'Source refund identifier',
  order_id STRING NOT NULL COMMENT 'Source order identifier retained as a degenerate dimension; not a foreign key to fct_orders',
  payment_id STRING NOT NULL COMMENT 'Source payment identifier retained as a degenerate dimension; not a foreign key to fct_payments',
  customer_key BIGINT NOT NULL COMMENT 'Enriched through the accepted order',
  campaign_key BIGINT NOT NULL COMMENT 'Enriched through the accepted order',
  currency_key BIGINT NOT NULL,
  refund_created_date_key INT NOT NULL,
  refund_processed_date_key INT NOT NULL,
  refund_reason STRING,
  refund_status STRING NOT NULL COMMENT 'TODO: define the accepted status domain after profiling Bronze',
  refund_amount DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0',
  created_at TIMESTAMP NOT NULL,
  processed_at TIMESTAMP NOT NULL COMMENT 'Must not precede created_at',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (refund_id)
  PRIMARY KEY (refund_key),
  FOREIGN KEY (customer_key) REFERENCES orderflow_dev.gold.dim_customers (customer_key),
  FOREIGN KEY (campaign_key) REFERENCES orderflow_dev.gold.dim_campaigns (campaign_key),
  FOREIGN KEY (currency_key) REFERENCES orderflow_dev.gold.dim_currency (currency_key),
  CONSTRAINT fk_fct_refunds_created_date FOREIGN KEY (refund_created_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key),
  CONSTRAINT fk_fct_refunds_processed_date FOREIGN KEY (refund_processed_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key)
)
USING DELTA
COMMENT 'Grain: one latest accepted refund per refund_id. Current-state refund fact.';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_shipments (
  shipment_key BIGINT NOT NULL,
  shipment_id STRING NOT NULL COMMENT 'Source shipment identifier',
  order_id STRING NOT NULL COMMENT 'Source order identifier retained as a degenerate dimension; not a foreign key to fct_orders',
  customer_key BIGINT NOT NULL COMMENT 'Enriched from the accepted order header',
  campaign_key BIGINT NOT NULL COMMENT 'Enriched from the accepted order header',
  shipped_date_key INT NOT NULL,
  estimated_delivery_date_key INT,
  delivered_date_key INT,
  carrier STRING NOT NULL,
  shipment_status STRING NOT NULL COMMENT 'One of: lost, shipped, delivered',
  shipped_at TIMESTAMP NOT NULL,
  delivered_at TIMESTAMP COMMENT 'Required when shipment_status = delivered; must be >= shipped_at',
  delivery_country_code STRING NOT NULL COMMENT 'Uppercase ISO 3166-1 alpha-2 country code',
  delivery_city STRING NOT NULL,
  shipping_cost DECIMAL(18, 2) NOT NULL COMMENT 'Must be >= 0. Source has no currency field; confirm its currency before cross-currency reporting',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (shipment_id)
  PRIMARY KEY (shipment_key),
  FOREIGN KEY (customer_key) REFERENCES orderflow_dev.gold.dim_customers (customer_key),
  FOREIGN KEY (campaign_key) REFERENCES orderflow_dev.gold.dim_campaigns (campaign_key),
  CONSTRAINT fk_fct_shipments_shipped_date FOREIGN KEY (shipped_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key),
  CONSTRAINT fk_fct_shipments_estimated_date FOREIGN KEY (estimated_delivery_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key),
  CONSTRAINT fk_fct_shipments_delivered_date FOREIGN KEY (delivered_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key)
)
USING DELTA
COMMENT 'Grain: one latest accepted shipment per shipment_id. Current-state/accumulating-snapshot fact.';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_web_events (
  event_key BIGINT NOT NULL,
  event_id STRING NOT NULL COMMENT 'Source web-event identifier',
  session_id STRING NOT NULL COMMENT 'Source session identifier retained as a degenerate dimension',
  anonymous_id STRING NOT NULL COMMENT 'Source visitor identifier retained as a degenerate dimension',
  customer_key BIGINT NOT NULL COMMENT 'Use the Anonymous or Unknown member when customer_id is null or unresolved',
  product_key BIGINT NOT NULL COMMENT 'Use the Not applicable member for non-product events and Unknown for unresolved product_id values',
  campaign_key BIGINT NOT NULL COMMENT 'Use the No campaign or Unknown member when campaign_id is null or unresolved',
  event_date_key INT NOT NULL,
  event_type STRING NOT NULL COMMENT 'TODO: define the accepted event-type domain after profiling Bronze',
  event_timestamp TIMESTAMP NOT NULL,
  device_type STRING NOT NULL COMMENT 'One of: tablet, mobile, desktop',
  country_code STRING NOT NULL COMMENT 'Event country; uppercase ISO 3166-1 alpha-2 code',
  page_url STRING NOT NULL,
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): UNIQUE (event_id)
  PRIMARY KEY (event_key),
  FOREIGN KEY (customer_key) REFERENCES orderflow_dev.gold.dim_customers (customer_key),
  FOREIGN KEY (product_key) REFERENCES orderflow_dev.gold.dim_products (product_key),
  FOREIGN KEY (campaign_key) REFERENCES orderflow_dev.gold.dim_campaigns (campaign_key),
  FOREIGN KEY (event_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key)
)
USING DELTA
COMMENT 'Grain: one accepted immutable web event per event_id';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.fct_exchange_rates (
  exchange_rate_key BIGINT NOT NULL,
  rate_date_key INT NOT NULL,
  currency_key BIGINT NOT NULL,
  rate_to_pln DECIMAL(18, 6) NOT NULL COMMENT 'PLN value of one unit of the source currency; must be > 0',
  source STRING NOT NULL COMMENT 'Rate provider or source name',
  _gold_processed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the row was produced or refreshed in Gold',
  -- DBML uniqueness (documentation only): CONSTRAINT uq_fct_exchange_rates_grain UNIQUE (rate_date_key, currency_key, source)
  PRIMARY KEY (exchange_rate_key),
  FOREIGN KEY (rate_date_key) REFERENCES orderflow_dev.gold.dim_calendar (date_key),
  FOREIGN KEY (currency_key) REFERENCES orderflow_dev.gold.dim_currency (currency_key)
)
USING DELTA
COMMENT 'Grain: one accepted exchange rate per rate date, currency, and source. Periodic-snapshot fact.';
