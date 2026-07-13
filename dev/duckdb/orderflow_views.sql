INSTALL delta;
LOAD delta;

CREATE OR REPLACE VIEW bronze_calendar AS
SELECT *
FROM delta_scan('data/bronze/calendar');

CREATE OR REPLACE VIEW silver_calendar AS
SELECT *
FROM delta_scan('data/silver/calendar');
