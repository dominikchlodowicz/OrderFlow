INSTALL delta;
LOAD delta;

CREATE OR REPLACE VIEW bronze_calendar AS
SELECT *
FROM delta_scan('data/bronze/calendar');

CREATE OR REPLACE VIEW silver_calendar AS
SELECT *
FROM delta_scan('data/silver/calendar');

CREATE OR REPLACE VIEW dim_calendar AS
SELECT *
FROM delta_scan('data/gold/dim_calendar');

CREATE OR REPLACE VIEW bronze_customers AS
SELECT *
FROM delta_scan('data/bronze/customers');

CREATE OR REPLACE VIEW silver_customers AS
SELECT *
FROM delta_scan('data/silver/customers');

CREATE OR REPLACE VIEW dim_customers AS
SELECT *
FROM delta_scan('data/gold/dim_customers');