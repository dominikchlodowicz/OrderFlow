# OrderFlow data quality

The final `gold_quality_checks` Lakeflow task validates the completed Gold dimensional model.
It uses native PySpark aggregations from `orderflow.quality`; no source dataset is collected to
the driver and no permanent results table is created. One small result row per check is retained
in the notebook report.

The final gate does not separately rescan Silver tables. Silver transformations
already enforce their own required-column, nullability, domain, and grain
contracts; this gate validates the persisted Gold outputs and their cross-table
relationships before the workflow can succeed.

## Scope and grain

| Table | Declared grain | Main checks |
| --- | --- | --- |
| `dim_calendar` | One row per `date_day` | `date_key`/`date_day` uniqueness, required date attributes, ranges, date-key consistency |
| `dim_customers` | One current row per `customer_id` (SCD1) | Surrogate/business-key uniqueness, regular-customer descriptions, status/active-flag consistency, special members |
| `dim_products` | One current row per `product_id` (SCD1) | Surrogate, product, and SKU uniqueness; names, price, currency, chronology, special members |
| `dim_campaigns` | One current row per `campaign_id` (SCD1) | Surrogate/business-key uniqueness, name, positive regular budget, channel, chronology, special members |
| `dim_currency` | One row per ISO currency | Surrogate/business-key uniqueness, code format, PLN reporting flag |
| `fct_orders` | Latest accepted row per `order_id` | Key uniqueness, statuses/channels, amounts, chronology, dimension references, fallback keys |
| `fct_order_items` | One row per `order_item_id` | Key uniqueness, positive quantity, non-negative amounts, line arithmetic, order/dimension references |
| `fct_payments` | Latest accepted row per `payment_id` | Key uniqueness, method/status/reason domains, positive attempt, non-negative amount, chronology and references |
| `fct_refunds` | Latest accepted row per `refund_id` | Key uniqueness, processed/rejected status, non-negative amount, chronology and references |
| `fct_shipments` | Latest accepted row per `shipment_id` | Key uniqueness, status, cost, delivery requirements/order, and references |
| `fct_web_events` | One immutable row per `event_id` | Key uniqueness, event/device domains, product applicability, fallback keys, and references |
| `fct_exchange_rates` | One row per date, currency, and source | Surrogate/grain uniqueness, positive rate, non-blank source, date/currency references |

Every table also has an existence, required-column, Spark-type, required-value, and non-empty
check. Every declared fact foreign key must resolve to the relevant dimension. The documented
negative customer, campaign, and product members are accepted only in the facts where their
meaning applies, and the special dimension rows themselves must be present with the correct key.

The refund and web-event accepted values come from the executable source generator and are also
present in the generated source data: refunds use `processed/rejected`; web events use
`page_view/product_view/add_to_cart/checkout_started/purchase_completed`. Order statuses follow
the generator and Silver contract:
`created/paid/shipped/delivered/cancelled/partially_refunded/refunded`.

## Cross-table checks

- Item, payment, refund, and shipment `order_id` values must resolve to `fct_orders`; refund
  `payment_id` values must resolve to `fct_payments`.
- Item and payment currency context must match the order; item creation cannot precede order
  creation, and a payment cannot exceed the order net amount. Payment-to-order chronology is not
  asserted because source authorization events can intentionally be backdated.
- Refund order/payment lineage and currency must agree, a refund cannot exceed either amount,
  and cumulative `processed` refunds cannot exceed the referenced payment.
- A shipment cannot precede order creation.
- Order headers reconcile to item gross, discount, and net totals only for PLN orders. Source
  item prices are PLN while non-PLN order headers are converted, so comparing them directly would
  be dimensionally invalid.

## Batch scope and failure propagation

The job passes `catalog`, `environment`, and an ISO `batch_id` (`YYYY-MM-DD`) to the notebook.
Current Gold transformations overwrite current-state snapshots and drop Bronze/Silver batch
lineage, so the final checks are table-wide. For this pipeline, the resulting Gold snapshot is
the output of the current run, making the non-empty checks output gates as well. Uniqueness and
referential-integrity rules are permanent invariants and must remain table-wide regardless.

Filtering by `batch_id` is intentionally not attempted: no Gold batch column exists and the
Bronze ingestion UUID is not derived from the Lakeflow `batch_id`. A future incremental design
should persist a shared batch identifier before adding batch-filtered checks.

The notebook prints every result, then raises `DataQualityFailure` if any critical result failed.
That exception fails `gold_quality_checks`, which fails the complete Lakeflow job. Airflow already
waits for that one job's terminal result, so it fails naturally without adding individual
Databricks transformation tasks to the DAG.

## Run locally

Install the development dependencies, then run the focused and complete suites:

```bash
python -m pip install -e ".[dev]"
python -m pytest tests/quality -q
python -m pytest -q
black --check src tests scripts
ruff check src tests scripts
databricks bundle validate -t dev
```

Local tests use small in-memory Spark DataFrames and require no Azure, ADLS, Unity Catalog,
Databricks cluster, or credentials.

For a controlled development run, validate first and then deliberately start the complete job:

```bash
databricks bundle validate -t dev --profile orderflow-dev
databricks bundle deploy -t dev --profile orderflow-dev
databricks bundle run -t dev --profile orderflow-dev orderflow_pipeline \
  --params catalog=orderflow_dev,environment=dev,batch_id=2026-06-30
```

Use the approved profile for the configured development workspace if it is not named
`orderflow-dev`.

## Add a check

For a reusable assertion, add or use a method on `DataQualitySuite`, register it in the relevant
table builder in `orderflow.quality.orderflow`, and add an in-memory Spark test. Row predicates
registered against the same DataFrame are aggregated together. Use `check_reference` for key
relationships; references for one source table are evaluated in one joined aggregation. Give the
check a stable, descriptive name and set `critical=False` only when a failure should be reported
without failing the job.
