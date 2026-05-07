from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
import argparse
import random
from typing import Any

import pandas as pd
from faker import Faker


PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class GeneratorConfig:
    """Configuration for the synthetic source-system generator."""

    start_date: str = "2026-01-01"
    end_date: str = "2026-03-31"
    output_dir: str = "data/raw"
    seed: int = 42

    initial_customers: int = 500
    initial_products: int = 120
    avg_orders_per_day: int = 80
    avg_web_sessions_per_day: int = 350

    payment_failure_rate: float = 0.12
    payment_retry_success_rate: float = 0.65
    refund_rate: float = 0.08
    partial_refund_rate: float = 0.45
    shipment_late_rate: float = 0.15
    shipment_lost_rate: float = 0.025
    shipment_returned_rate: float = 0.035

    customer_update_rate: float = 0.03
    product_update_rate: float = 0.025
    late_arrival_rate: float = 0.07
    invalid_record_rate: float = 0.012
    duplicate_rate: float = 0.008

    include_empty_files: bool = True
    clean_output: bool = True


class OrderFlowGenerator:
    """
    Synthetic source-system simulator for a production-style ecommerce analytics project.

    Produces daily partitioned CSV files for:
    - customers
    - products
    - marketing_campaigns
    - calendar
    - exchange_rates
    - orders
    - order_items
    - payments
    - shipments
    - refunds
    - web_events

    Important behavior:
    - stable master data
    - daily business events
    - late-arriving records using event timestamps vs loaded_at
    - customer/product changes for dbt snapshots
    - realistic relationships between orders, items, payments, shipments, refunds
    - controlled bad records for dbt/data-quality tests
    - partitioned files for ingestion by load_date
    """

    COUNTRIES = ["PL", "DE", "CZ", "SK", "LT"]
    CURRENCIES_BY_COUNTRY = {
        "PL": "PLN",
        "DE": "EUR",
        "CZ": "CZK",
        "SK": "EUR",
        "LT": "EUR",
    }
    CITIES_BY_COUNTRY = {
        "PL": ["Warszawa", "Kraków", "Wrocław", "Gdańsk", "Poznań", "Łódź"],
        "DE": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"],
        "CZ": ["Prague", "Brno", "Ostrava", "Plzeň"],
        "SK": ["Bratislava", "Košice", "Žilina", "Nitra"],
        "LT": ["Vilnius", "Kaunas", "Klaipėda", "Šiauliai"],
    }
    PRODUCT_CATEGORIES = ["electronics", "home", "books", "beauty", "sports", "fashion"]
    BRANDS = ["VistulaTech", "NordHome", "Bookly", "FitPeak", "UrbanFox", "BalticGoods"]
    PAYMENT_METHODS = ["card", "blik", "bank_transfer", "paypal"]
    CARRIERS = ["InPost", "DPD", "DHL", "UPS", "Poczta Polska"]
    DEVICES = ["desktop", "mobile", "tablet"]
    EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout_started", "purchase_completed"]
    SOURCE_CHANNELS = ["organic", "paid_search", "email", "social", "direct"]

    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self.fake = Faker(["pl_PL", "en_US"])
        Faker.seed(config.seed)
        random.seed(config.seed)

        output_dir = Path(config.output_dir)
        self.output_root = output_dir if output_dir.is_absolute() else PROJECT_ROOT / output_dir

        self.customers: list[dict[str, Any]] = []
        self.products: list[dict[str, Any]] = []
        self.marketing_campaigns: list[dict[str, Any]] = []

        self.orders_by_id: dict[str, dict[str, Any]] = {}
        self.order_items_by_order_id: dict[str, list[dict[str, Any]]] = {}
        self.payments_by_order_id: dict[str, list[dict[str, Any]]] = {}
        self.shipments_by_order_id: dict[str, list[dict[str, Any]]] = {}
        self.refunds_by_order_id: dict[str, list[dict[str, Any]]] = {}

        self.scheduled_events: list[dict[str, Any]] = []
        self.base_exchange_rates_to_pln = {
            "PLN": 1.0,
            "EUR": 4.32,
            "CZK": 0.17,
        }

    def generate(self) -> None:
        if self.config.clean_output and self.output_root.exists():
            self._remove_output_tree(self.output_root)

        self._generate_initial_customers()
        self._generate_initial_products()
        self._generate_marketing_campaigns()

        all_dates = [d.date() for d in pd.date_range(self.config.start_date, self.config.end_date)]

        for load_date in all_dates:
            rows: dict[str, list[dict[str, Any]]] = {
                "customers": [],
                "products": [],
                "marketing_campaigns": [],
                "calendar": [],
                "exchange_rates": [],
                "orders": [],
                "order_items": [],
                "payments": [],
                "shipments": [],
                "refunds": [],
                "web_events": [],
            }

            rows["calendar"] = [self._calendar_row(load_date)]
            rows["exchange_rates"] = self._exchange_rate_rows(load_date)

            if load_date == all_dates[0]:
                rows["customers"].extend(self._as_loaded_rows(self.customers, load_date))
                rows["products"].extend(self._as_loaded_rows(self.products, load_date))
                rows["marketing_campaigns"].extend(self._as_loaded_rows(self.marketing_campaigns, load_date))
            else:
                rows["customers"].extend(self._generate_new_customers(load_date))
                rows["customers"].extend(self._generate_customer_updates(load_date))
                rows["products"].extend(self._generate_product_updates(load_date))

            due_events = self._pop_due_events(load_date)
            self._materialize_scheduled_events(due_events, load_date, rows)

            daily_orders, daily_order_items = self._generate_daily_orders(load_date)
            rows["orders"].extend(daily_orders)
            rows["order_items"].extend(daily_order_items)

            daily_payments = self._generate_payments(daily_orders, load_date)
            rows["payments"].extend(daily_payments)

            rows["web_events"].extend(self._generate_web_events(load_date, daily_orders))

            self._inject_quality_issues(rows)

            for table_name, table_rows in rows.items():
                self._write_partition(table_name, load_date, table_rows)

    def _remove_output_tree(self, path: Path) -> None:
        for child in path.glob("**/*"):
            if child.is_file():
                child.unlink()
        for child in sorted(path.glob("**/*"), reverse=True):
            if child.is_dir():
                child.rmdir()
        path.rmdir()

    def _generate_initial_customers(self) -> None:
        statuses = ["active", "active", "active", "inactive"]

        for _ in range(self.config.initial_customers):
            country = random.choice(self.COUNTRIES)
            created_at = self.fake.date_time_between(start_date="-2y", end_date="-30d")
            customer = {
                "customer_id": f"cust_{uuid4().hex[:12]}",
                "email": self.fake.unique.email(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "country_code": country,
                "city": random.choice(self.CITIES_BY_COUNTRY[country]),
                "created_at": created_at,
                "updated_at": created_at,
                "customer_status": random.choice(statuses),
                "marketing_consent": random.choice([True, False]),
            }
            self.customers.append(customer)

    def _generate_initial_products(self) -> None:
        for i in range(self.config.initial_products):
            category = random.choice(self.PRODUCT_CATEGORIES)
            brand = random.choice(self.BRANDS)
            created_at = self.fake.date_time_between(start_date="-3y", end_date="-60d")
            product = {
                "product_id": f"prod_{i + 1:05d}",
                "sku": f"SKU-{i + 1:05d}",
                "product_name": f"{brand} {category.title()} {self.fake.word().title()}",
                "category": category,
                "brand": brand,
                "unit_price": round(random.uniform(20, 800), 2),
                "currency": "PLN",
                "is_active": True,
                "created_at": created_at,
                "updated_at": created_at,
            }
            self.products.append(product)

    def _generate_marketing_campaigns(self) -> None:
        campaigns = [
            ("cmp_winter", "Winter Sale", "paid_search", "2026-01-01", "2026-01-21", 7000),
            ("cmp_student", "Student Promo", "social", "2026-01-10", "2026-02-15", 5500),
            ("cmp_valentine", "Valentine Campaign", "email", "2026-02-01", "2026-02-16", 4000),
            ("cmp_spring", "Spring Launch", "paid_search", "2026-03-01", "2026-03-31", 9000),
            ("cmp_retention", "Retention Email Series", "email", "2026-01-01", "2026-03-31", 3000),
        ]

        for campaign_id, name, channel, start_date, end_date, budget in campaigns:
            self.marketing_campaigns.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": name,
                    "source_channel": channel,
                    "start_date": start_date,
                    "end_date": end_date,
                    "budget_amount": budget,
                    "currency": "PLN",
                    "created_at": pd.Timestamp(start_date) - pd.Timedelta(days=10),
                    "updated_at": pd.Timestamp(start_date) - pd.Timedelta(days=10),
                    "is_active": True,
                }
            )

    def _generate_new_customers(self, load_date) -> list[dict[str, Any]]:
        new_count = random.randint(3, 15)
        rows = []

        for _ in range(new_count):
            country = random.choice(self.COUNTRIES)
            created_at = self._random_timestamp_on(load_date)
            customer = {
                "customer_id": f"cust_{uuid4().hex[:12]}",
                "email": self.fake.unique.email(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "country_code": country,
                "city": random.choice(self.CITIES_BY_COUNTRY[country]),
                "created_at": created_at,
                "updated_at": created_at,
                "customer_status": "active",
                "marketing_consent": random.choice([True, False]),
            }
            self.customers.append(customer)
            rows.append(self._with_load_metadata(customer, load_date, created_at))

        return rows

    def _generate_customer_updates(self, load_date) -> list[dict[str, Any]]:
        rows = []
        sample_size = min(len(self.customers), max(1, int(len(self.customers) * self.config.customer_update_rate)))

        for customer in random.sample(self.customers, sample_size):
            if random.random() > 0.75:
                continue

            updated = customer.copy()
            change_type = random.choice(["city", "marketing_consent", "inactive"])

            if change_type == "city":
                updated["city"] = random.choice(self.CITIES_BY_COUNTRY[updated["country_code"]])
            elif change_type == "marketing_consent":
                updated["marketing_consent"] = not bool(updated["marketing_consent"])
            elif change_type == "inactive":
                updated["customer_status"] = "inactive"

            updated_at = self._random_timestamp_on(load_date)
            updated["updated_at"] = updated_at
            customer.update(updated)
            rows.append(self._with_load_metadata(updated, load_date, updated_at))

        return rows

    def _generate_product_updates(self, load_date) -> list[dict[str, Any]]:
        rows = []
        sample_size = min(len(self.products), max(1, int(len(self.products) * self.config.product_update_rate)))

        for product in random.sample(self.products, sample_size):
            updated = product.copy()
            change_type = random.choice(["price", "category", "inactive"])

            if change_type == "price":
                updated["unit_price"] = round(float(product["unit_price"]) * random.uniform(0.9, 1.15), 2)
            elif change_type == "category":
                updated["category"] = random.choice(self.PRODUCT_CATEGORIES)
            elif change_type == "inactive":
                updated["is_active"] = False

            updated_at = self._random_timestamp_on(load_date)
            updated["updated_at"] = updated_at
            product.update(updated)
            rows.append(self._with_load_metadata(updated, load_date, updated_at))

        return rows

    def _generate_daily_orders(self, load_date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        multiplier = self._daily_order_multiplier(load_date)
        order_count = max(0, int(random.gauss(self.config.avg_orders_per_day * multiplier, 15)))
        orders = []
        order_items = []

        active_customers = [c for c in self.customers if c["customer_status"] == "active"]
        active_products = [p for p in self.products if p["is_active"]]

        for _ in range(order_count):
            customer = random.choice(active_customers)
            order_id = f"ord_{uuid4().hex[:12]}"
            order_created_at = self._random_timestamp_on(load_date)
            campaign_id = self._choose_campaign(load_date)
            source_channel = self._campaign_channel(campaign_id) if campaign_id else random.choice(self.SOURCE_CHANNELS)

            item_count = random.randint(1, 5)
            selected_products = random.sample(active_products, k=min(item_count, len(active_products)))

            gross_amount = 0.0
            discount_amount = 0.0
            items_for_order = []

            for item_number, product in enumerate(selected_products, start=1):
                quantity = random.choices([1, 2, 3, 4], weights=[65, 20, 10, 5])[0]
                unit_price = float(product["unit_price"])
                line_discount = round(unit_price * quantity * random.choice([0, 0, 0.05, 0.10]), 2)
                line_total = round(unit_price * quantity - line_discount, 2)

                gross_amount += unit_price * quantity
                discount_amount += line_discount

                item = {
                    "order_item_id": f"{order_id}_{item_number}",
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_amount": line_discount,
                    "line_total": line_total,
                    "created_at": order_created_at,
                }
                items_for_order.append(item)
                order_items.append(self._with_load_metadata(item, load_date, order_created_at))

            currency = self.CURRENCIES_BY_COUNTRY[customer["country_code"]]
            net_amount_pln = round(gross_amount - discount_amount, 2)
            net_amount = self._convert_from_pln(net_amount_pln, currency)
            gross_amount_converted = self._convert_from_pln(round(gross_amount, 2), currency)
            discount_amount_converted = self._convert_from_pln(round(discount_amount, 2), currency)

            order = {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_status": "created",
                "order_created_at": order_created_at,
                "order_updated_at": order_created_at,
                "country_code": customer["country_code"],
                "currency": currency,
                "gross_amount": gross_amount_converted,
                "discount_amount": discount_amount_converted,
                "net_amount": net_amount,
                "source_channel": source_channel,
                "campaign_id": campaign_id,
            }

            self.orders_by_id[order_id] = order.copy()
            self.order_items_by_order_id[order_id] = items_for_order
            orders.append(self._with_load_metadata(order, load_date, order_created_at))

        return orders, order_items

    def _generate_payments(self, daily_orders: list[dict[str, Any]], load_date) -> list[dict[str, Any]]:
        rows = []

        for loaded_order in daily_orders:
            order_id = loaded_order["order_id"]
            order = self.orders_by_id[order_id]
            payment_created_at = order["order_created_at"] + pd.Timedelta(minutes=random.randint(1, 30))
            first_failed = random.random() < self.config.payment_failure_rate

            if first_failed:
                failed_payment = self._payment_row(
                    order=order,
                    attempt_number=1,
                    status="failed",
                    created_at=payment_created_at,
                    failure_reason=random.choice(["insufficient_funds", "card_declined", "timeout"]),
                )
                self._record_payment(failed_payment)
                rows.append(self._maybe_late_loaded_row(failed_payment, load_date, payment_created_at, "payments"))

                if random.random() < self.config.payment_retry_success_rate:
                    second_created_at = payment_created_at + pd.Timedelta(minutes=random.randint(5, 90))
                    captured_payment = self._payment_row(
                        order=order,
                        attempt_number=2,
                        status="captured",
                        created_at=second_created_at,
                        failure_reason=None,
                    )
                    self._record_payment(captured_payment)
                    rows.append(self._maybe_late_loaded_row(captured_payment, load_date, second_created_at, "payments"))
                    self._schedule_order_update(order_id, "paid", second_created_at, load_date)
                    self._schedule_authorized_status_if_needed(order, second_created_at, load_date)
                    self._schedule_shipment(order_id, second_created_at, load_date)
                else:
                    self._schedule_order_update(order_id, "cancelled", payment_created_at, load_date)
            else:
                captured_payment = self._payment_row(
                    order=order,
                    attempt_number=1,
                    status="captured",
                    created_at=payment_created_at,
                    failure_reason=None,
                )
                self._record_payment(captured_payment)
                rows.append(self._maybe_late_loaded_row(captured_payment, load_date, payment_created_at, "payments"))
                self._schedule_authorized_status_if_needed(order, payment_created_at, load_date)
                self._schedule_order_update(order_id, "paid", payment_created_at, load_date)
                self._schedule_shipment(order_id, payment_created_at, load_date)

        return self._keep_rows_due_today(rows, load_date)

    def _payment_row(
        self,
        order: dict[str, Any],
        attempt_number: int,
        status: str,
        created_at: pd.Timestamp,
        failure_reason: str | None,
    ) -> dict[str, Any]:
        return {
            "payment_id": f"pay_{uuid4().hex[:12]}",
            "order_id": order["order_id"],
            "payment_attempt_number": attempt_number,
            "payment_method": random.choice(self.PAYMENT_METHODS),
            "payment_status": status,
            "amount": order["net_amount"],
            "currency": order["currency"],
            "created_at": created_at,
            "processed_at": created_at + pd.Timedelta(seconds=random.randint(5, 60)),
            "failure_reason": failure_reason,
        }

    def _record_payment(self, payment: dict[str, Any]) -> None:
        self.payments_by_order_id.setdefault(payment["order_id"], []).append(payment.copy())

    def _schedule_authorized_status_if_needed(self, order: dict[str, Any], captured_at: pd.Timestamp, load_date) -> None:
        if random.random() < 0.28:
            authorized_at = captured_at - pd.Timedelta(seconds=random.randint(30, 240))
            authorized_payment = self._payment_row(
                order=order,
                attempt_number=1,
                status="authorized",
                created_at=authorized_at,
                failure_reason=None,
            )
            authorized_payment["amount"] = order["net_amount"]
            self._record_payment(authorized_payment)
            self._schedule_row("payments", authorized_payment, authorized_at, load_date)

    def _schedule_order_update(self, order_id: str, status: str, event_timestamp: pd.Timestamp, current_load_date) -> None:
        order = self.orders_by_id[order_id].copy()
        order["order_status"] = status
        order["order_updated_at"] = event_timestamp
        self.orders_by_id[order_id].update(order)
        self._schedule_row("orders", order, event_timestamp, current_load_date)

    def _schedule_shipment(self, order_id: str, payment_timestamp: pd.Timestamp, current_load_date) -> None:
        order = self.orders_by_id[order_id]
        shipped_at = payment_timestamp + pd.Timedelta(hours=random.randint(4, 48))
        estimated_delivery_at = shipped_at + pd.Timedelta(days=random.randint(1, 4))
        is_late = random.random() < self.config.shipment_late_rate
        lost = random.random() < self.config.shipment_lost_rate
        returned = not lost and random.random() < self.config.shipment_returned_rate

        shipment = {
            "shipment_id": f"ship_{uuid4().hex[:12]}",
            "order_id": order_id,
            "carrier": random.choice(self.CARRIERS),
            "shipment_status": "shipped",
            "shipped_at": shipped_at,
            "estimated_delivery_at": estimated_delivery_at,
            "delivered_at": None,
            "delivery_country": order["country_code"],
            "delivery_city": self._city_for_country(order["country_code"]),
            "shipping_cost": round(random.uniform(8, 35), 2),
        }
        self.shipments_by_order_id.setdefault(order_id, []).append(shipment.copy())
        self._schedule_row("shipments", shipment, shipped_at, current_load_date)
        self._schedule_order_update(order_id, "shipped", shipped_at, current_load_date)

        if lost:
            lost_at = shipped_at + pd.Timedelta(days=random.randint(2, 7))
            lost_shipment = shipment.copy()
            lost_shipment["shipment_status"] = "lost"
            self._schedule_row("shipments", lost_shipment, lost_at, current_load_date)
            return

        delivered_at = estimated_delivery_at + pd.Timedelta(days=random.randint(1, 3) if is_late else 0)
        delivered_shipment = shipment.copy()
        delivered_shipment["shipment_status"] = "delivered"
        delivered_shipment["delivered_at"] = delivered_at
        self._schedule_row("shipments", delivered_shipment, delivered_at, current_load_date)
        self._schedule_order_update(order_id, "delivered", delivered_at, current_load_date)

        if returned:
            returned_at = delivered_at + pd.Timedelta(days=random.randint(2, 10))
            returned_shipment = delivered_shipment.copy()
            returned_shipment["shipment_status"] = "returned"
            self._schedule_row("shipments", returned_shipment, returned_at, current_load_date)

        if random.random() < self.config.refund_rate:
            self._schedule_refund(order_id, delivered_at, current_load_date)

    def _schedule_refund(self, order_id: str, delivered_at: pd.Timestamp, current_load_date) -> None:
        order = self.orders_by_id[order_id]
        payment = self._last_captured_payment(order_id)
        if payment is None:
            return

        refund_created_at = delivered_at + pd.Timedelta(days=random.randint(2, 14))
        is_partial = random.random() < self.config.partial_refund_rate
        refund_amount = (
            round(float(order["net_amount"]) * random.uniform(0.15, 0.65), 2)
            if is_partial
            else float(order["net_amount"])
        )
        refund_status = random.choice(["processed", "processed", "processed", "rejected"])

        refund = {
            "refund_id": f"ref_{uuid4().hex[:12]}",
            "order_id": order_id,
            "payment_id": payment["payment_id"],
            "refund_reason": random.choice(["damaged", "wrong_size", "late_delivery", "customer_changed_mind"]),
            "refund_amount": refund_amount,
            "currency": order["currency"],
            "created_at": refund_created_at,
            "processed_at": refund_created_at + pd.Timedelta(hours=random.randint(1, 48)),
            "refund_status": refund_status,
        }

        self.refunds_by_order_id.setdefault(order_id, []).append(refund.copy())
        self._schedule_row("refunds", refund, refund_created_at, current_load_date)

        if refund_status == "processed":
            refund_payment = {
                "payment_id": f"pay_{uuid4().hex[:12]}",
                "order_id": order_id,
                "payment_attempt_number": payment["payment_attempt_number"],
                "payment_method": payment["payment_method"],
                "payment_status": "refunded",
                "amount": refund_amount,
                "currency": order["currency"],
                "created_at": refund_created_at,
                "processed_at": refund["processed_at"],
                "failure_reason": None,
            }
            self._record_payment(refund_payment)
            self._schedule_row("payments", refund_payment, refund_created_at, current_load_date)
            new_order_status = "partially_refunded" if is_partial else "refunded"
            self._schedule_order_update(order_id, new_order_status, refund["processed_at"], current_load_date)

    def _last_captured_payment(self, order_id: str) -> dict[str, Any] | None:
        payments = [
            p for p in self.payments_by_order_id.get(order_id, [])
            if p["payment_status"] == "captured"
        ]
        return payments[-1] if payments else None

    def _generate_web_events(self, load_date, daily_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        session_count = max(0, int(random.gauss(self.config.avg_web_sessions_per_day, 60)))
        active_customers = [c for c in self.customers if c["customer_status"] == "active"]
        active_products = [p for p in self.products if p["is_active"]]
        purchased_order_ids = {o["order_id"] for o in daily_orders}

        for _ in range(session_count):
            known_customer = random.random() < 0.55 and bool(active_customers)
            customer = random.choice(active_customers) if known_customer else None
            country = customer["country_code"] if customer else random.choice(self.COUNTRIES)
            campaign_id = self._choose_campaign(load_date) if random.random() < 0.35 else None
            session_id = f"sess_{uuid4().hex[:12]}"
            anonymous_id = f"anon_{uuid4().hex[:12]}"
            session_start = self._random_timestamp_on(load_date)
            event_count = random.randint(1, 8)
            checkout_happened = False

            for event_index in range(event_count):
                if event_index == 0:
                    event_type = "page_view"
                else:
                    event_type = random.choices(
                        self.EVENT_TYPES,
                        weights=[45, 28, 14, 8, 5],
                    )[0]
                if event_type == "checkout_started":
                    checkout_happened = True
                if event_type == "purchase_completed" and not checkout_happened:
                    event_type = "product_view"

                product = random.choice(active_products) if active_products and event_type in {"product_view", "add_to_cart"} else None
                event_timestamp = session_start + pd.Timedelta(minutes=random.randint(0, 90), seconds=random.randint(0, 59))

                row = {
                    "event_id": f"evt_{uuid4().hex[:16]}",
                    "session_id": session_id,
                    "customer_id": customer["customer_id"] if customer else None,
                    "anonymous_id": anonymous_id,
                    "event_type": event_type,
                    "event_timestamp": event_timestamp,
                    "product_id": product["product_id"] if product else None,
                    "campaign_id": campaign_id,
                    "device_type": random.choice(self.DEVICES),
                    "country_code": country,
                    "page_url": self._page_url(event_type, product),
                }
                rows.append(self._maybe_late_loaded_row(row, load_date, event_timestamp, "web_events"))

        for order in random.sample(daily_orders, k=min(len(daily_orders), max(0, len(purchased_order_ids) // 4))):
            event_timestamp = order["order_created_at"] - pd.Timedelta(minutes=random.randint(1, 20))
            row = {
                "event_id": f"evt_{uuid4().hex[:16]}",
                "session_id": f"sess_{uuid4().hex[:12]}",
                "customer_id": order["customer_id"],
                "anonymous_id": f"anon_{uuid4().hex[:12]}",
                "event_type": "purchase_completed",
                "event_timestamp": event_timestamp,
                "product_id": None,
                "campaign_id": order["campaign_id"],
                "device_type": random.choice(self.DEVICES),
                "country_code": order["country_code"],
                "page_url": "/checkout/success",
            }
            rows.append(self._maybe_late_loaded_row(row, load_date, event_timestamp, "web_events"))

        return self._keep_rows_due_today(rows, load_date)

    def _schedule_row(self, table_name: str, row: dict[str, Any], event_timestamp: pd.Timestamp, current_load_date) -> None:
        arrival_date = self._arrival_date(event_timestamp, current_load_date)
        self.scheduled_events.append(
            {
                "table_name": table_name,
                "arrival_date": arrival_date,
                "event_timestamp": event_timestamp,
                "row": row.copy(),
            }
        )

    def _maybe_late_loaded_row(
        self,
        row: dict[str, Any],
        current_load_date,
        event_timestamp: pd.Timestamp,
        table_name: str,
    ) -> dict[str, Any]:
        arrival_date = self._arrival_date(event_timestamp, current_load_date)
        loaded = self._with_load_metadata(row, arrival_date, event_timestamp)
        if arrival_date == current_load_date:
            return loaded
        self.scheduled_events.append(
            {
                "table_name": table_name,
                "arrival_date": arrival_date,
                "event_timestamp": event_timestamp,
                "row": row.copy(),
            }
        )
        loaded["__defer_until__"] = arrival_date
        return loaded

    def _keep_rows_due_today(self, rows: list[dict[str, Any]], load_date) -> list[dict[str, Any]]:
        return [r for r in rows if r.get("__defer_until__") in (None, load_date)]

    def _pop_due_events(self, load_date) -> list[dict[str, Any]]:
        due = [e for e in self.scheduled_events if e["arrival_date"] <= load_date]
        self.scheduled_events = [e for e in self.scheduled_events if e["arrival_date"] > load_date]
        return due

    def _materialize_scheduled_events(self, events: list[dict[str, Any]], load_date, rows: dict[str, list[dict[str, Any]]]) -> None:
        for event in events:
            table_name = event["table_name"]
            row = self._with_load_metadata(event["row"], load_date, event["event_timestamp"])
            rows[table_name].append(row)

    def _arrival_date(self, event_timestamp: pd.Timestamp, current_load_date) -> Any:
        base_arrival_date = max(event_timestamp.date(), current_load_date)
        if random.random() < self.config.late_arrival_rate:
            return (pd.Timestamp(base_arrival_date) + pd.Timedelta(days=random.randint(1, 3))).date()
        return base_arrival_date

    def _inject_quality_issues(self, rows: dict[str, list[dict[str, Any]]]) -> None:
        if rows["order_items"] and random.random() < self.config.invalid_record_rate:
            random.choice(rows["order_items"])["quantity"] = -1

        if rows["orders"] and random.random() < self.config.invalid_record_rate:
            random.choice(rows["orders"])["customer_id"] = "missing_customer"

        if rows["orders"] and random.random() < self.config.invalid_record_rate:
            random.choice(rows["orders"])["country_code"] = "XX"

        if rows["payments"] and random.random() < self.config.invalid_record_rate:
            payment = random.choice(rows["payments"])
            payment["amount"] = round(float(payment["amount"]) * random.uniform(0.3, 1.8), 2)

        if rows["shipments"] and random.random() < self.config.invalid_record_rate:
            shipment = random.choice(rows["shipments"])
            if shipment.get("delivered_at") not in (None, ""):
                shipment["delivered_at"] = pd.Timestamp(shipment["shipped_at"]) - pd.Timedelta(days=1)

        if rows["refunds"] and random.random() < self.config.invalid_record_rate:
            random.choice(rows["refunds"])["payment_id"] = "missing_payment"

        for table_name in ["payments", "web_events", "orders"]:
            if rows[table_name] and random.random() < self.config.duplicate_rate:
                rows[table_name].append(random.choice(rows[table_name]).copy())

    def _calendar_row(self, date_value) -> dict[str, Any]:
        timestamp = pd.Timestamp(date_value)
        polish_holidays = {
            "2026-01-01": "New Year's Day",
            "2026-01-06": "Epiphany",
            "2026-04-05": "Easter Sunday",
            "2026-04-06": "Easter Monday",
            "2026-05-01": "Labour Day",
            "2026-05-03": "Constitution Day",
            "2026-12-25": "Christmas Day",
            "2026-12-26": "Second Day of Christmas",
        }
        date_str = str(date_value)
        return {
            "date_day": date_value,
            "year": timestamp.year,
            "quarter": timestamp.quarter,
            "month": timestamp.month,
            "day_of_month": timestamp.day,
            "day_of_week": timestamp.dayofweek + 1,
            "day_name": timestamp.day_name(),
            "week_of_year": int(timestamp.isocalendar().week),
            "is_weekend": timestamp.dayofweek >= 5,
            "is_polish_public_holiday": date_str in polish_holidays,
            "holiday_name": polish_holidays.get(date_str),
            "load_date": date_value,
            "loaded_at": pd.Timestamp(date_value) + pd.Timedelta(hours=1),
        }

    def _exchange_rate_rows(self, load_date) -> list[dict[str, Any]]:
        rows = []
        for currency, base_rate in self.base_exchange_rates_to_pln.items():
            fluctuation = 1 if currency == "PLN" else random.uniform(0.985, 1.015)
            rows.append(
                {
                    "rate_date": load_date,
                    "currency": currency,
                    "rate_to_pln": round(base_rate * fluctuation, 4),
                    "source": "synthetic_nbp_like",
                    "load_date": load_date,
                    "loaded_at": pd.Timestamp(load_date) + pd.Timedelta(hours=2),
                }
            )
        return rows

    def _as_loaded_rows(self, rows: list[dict[str, Any]], load_date) -> list[dict[str, Any]]:
        return [self._with_load_metadata(row, load_date, row.get("updated_at") or row.get("created_at") or pd.Timestamp(load_date)) for row in rows]

    def _with_load_metadata(self, row: dict[str, Any], load_date, event_timestamp: Any | None = None) -> dict[str, Any]:
        result = row.copy()
        result["load_date"] = load_date
        result["loaded_at"] = pd.Timestamp(load_date) + pd.Timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        if event_timestamp is not None:
            result["source_event_at"] = event_timestamp
        return result

    def _write_partition(self, table_name: str, load_date, rows: list[dict[str, Any]]) -> None:
        output_path = self.output_root / table_name / f"load_date={load_date}"
        output_path.mkdir(parents=True, exist_ok=True)

        cleaned_rows = []
        for row in rows:
            cleaned = {k: v for k, v in row.items() if not k.startswith("__")}
            cleaned_rows.append(cleaned)

        df = pd.DataFrame(cleaned_rows)
        if not self.config.include_empty_files and df.empty:
            return

        file_path = output_path / f"{table_name}.csv"
        df.to_csv(file_path, index=False)

    def _random_timestamp_on(self, date_value) -> pd.Timestamp:
        return pd.Timestamp(date_value) + pd.Timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

    def _daily_order_multiplier(self, date_value) -> float:
        date = pd.Timestamp(date_value)
        multiplier = 1.0
        if date.dayofweek in [4, 5, 6]:
            multiplier *= 1.35
        if date.month == 1 and date.day <= 21:
            multiplier *= 1.15
        if date.month == 2 and 1 <= date.day <= 16:
            multiplier *= 1.10
        if date.month == 3:
            multiplier *= 1.20
        if str(date_value) in {"2026-01-01", "2026-01-06"}:
            multiplier *= 0.65
        return multiplier

    def _choose_campaign(self, date_value) -> str | None:
        active_campaigns = [
            c for c in self.marketing_campaigns
            if pd.Timestamp(c["start_date"]).date() <= date_value <= pd.Timestamp(c["end_date"]).date()
        ]
        if not active_campaigns or random.random() > 0.42:
            return None
        return random.choice(active_campaigns)["campaign_id"]

    def _campaign_channel(self, campaign_id: str | None) -> str:
        for campaign in self.marketing_campaigns:
            if campaign["campaign_id"] == campaign_id:
                return campaign["source_channel"]
        return random.choice(self.SOURCE_CHANNELS)

    def _city_for_country(self, country_code: str) -> str:
        return random.choice(self.CITIES_BY_COUNTRY.get(country_code, [self.fake.city()]))

    def _page_url(self, event_type: str, product: dict[str, Any] | None) -> str:
        if event_type == "page_view":
            return random.choice(["/", "/search", "/category/fashion", "/category/electronics"])
        if event_type in {"product_view", "add_to_cart"} and product:
            return f"/product/{product['sku']}"
        if event_type == "checkout_started":
            return "/checkout"
        if event_type == "purchase_completed":
            return "/checkout/success"
        return "/"

    def _convert_from_pln(self, amount_pln: float, currency: str) -> float:
        rate_to_pln = self.base_exchange_rates_to_pln[currency]
        return round(amount_pln / rate_to_pln, 2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate OrderFlow synthetic ecommerce source data.")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-03-31")
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--avg-orders-per-day", type=int, default=80)
    parser.add_argument("--avg-web-sessions-per-day", type=int, default=350)
    parser.add_argument("--no-clean", action="store_true", help="Do not delete existing output before generation.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = GeneratorConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        seed=args.seed,
        avg_orders_per_day=args.avg_orders_per_day,
        avg_web_sessions_per_day=args.avg_web_sessions_per_day,
        clean_output=not args.no_clean,
    )
    generator = OrderFlowGenerator(config)
    generator.generate()
    print(f"Generated synthetic source data in: {generator.output_root}")

