%sql


CREATE CATALOG IF NOT EXISTS orderflow_dev
COMMENT 'Orderflow development catalog';

CREATE SCHEMA IF NOT EXISTS orderflow_dev.bronze
COMMENT 'Raw and minimally processed Orderflow data';

CREATE SCHEMA IF NOT EXISTS orderflow_dev.silver
COMMENT 'Cleaned and validated Orderflow data';

CREATE SCHEMA IF NOT EXISTS orderflow_dev.gold
COMMENT 'Dimensional and analytical Orderflow data';

CREATE EXTERNAL VOLUME IF NOT EXISTS orderflow_dev.bronze.landing
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/bronze/landing'
COMMENT 'Raw source files delivered to Orderflow';
