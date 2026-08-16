%sql

-- COMMAND ----------
CREATE EXTERNAL LOCATION IF NOT EXISTS orderflow_managed_location
URL 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/_uc_managed'
WITH (STORAGE CREDENTIAL orderflow_adls_credential)
COMMENT 'Managed ADLS Gen2 storage for Orderflow Unity Catalog objects';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS orderflow_landing_location
URL 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/landing'
WITH (STORAGE CREDENTIAL orderflow_adls_credential)
COMMENT 'Externally delivered Orderflow source files';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS orderflow_dev
MANAGED LOCATION
  'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/_uc_managed/orderflow_dev'
COMMENT 'Orderflow development catalog';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS orderflow_dev.bronze
COMMENT 'Raw and minimally processed Orderflow data';

CREATE SCHEMA IF NOT EXISTS orderflow_dev.silver
COMMENT 'Cleaned, validated, and conformed Orderflow data';

CREATE SCHEMA IF NOT EXISTS orderflow_dev.gold
COMMENT 'Dimensional and analytical Orderflow data';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS orderflow_dev.bronze.landing
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/landing'
COMMENT 'Raw source files delivered to Orderflow';

-- COMMAND ----------

-- Bootstrap verification.
DESCRIBE EXTERNAL LOCATION orderflow_managed_location;
DESCRIBE EXTERNAL LOCATION orderflow_landing_location;
DESCRIBE CATALOG EXTENDED orderflow_dev;
SHOW SCHEMAS IN orderflow_dev;
DESCRIBE VOLUME orderflow_dev.bronze.landing;