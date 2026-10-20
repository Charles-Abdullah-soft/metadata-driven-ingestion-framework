"""
synthetic_ecommerce.py
Reusable synthetic e-commerce data generator (dbldatagen + Faker).

Usage:
    cfg    = build_config(preset="small", orders=1000, seed=7)
    tables = generate_all_tables(spark, cfg)          # dict[str, DataFrame]
"""
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import dbldatagen as dg
from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

# =====================================================================================
# 1. DEFAULTS (override any of these through build_config)
# =====================================================================================

PRESETS = {
    "tiny":   dict(customers=5,         products=8,      categories=4,   orders=10),
    "small":  dict(customers=100,       products=50,     categories=12,  orders=300),
    "medium": dict(customers=10_000,    products=2_000,  categories=50,  orders=50_000),
    "large":  dict(customers=1_000_000, products=50_000, categories=200, orders=5_000_000),
}

DEFAULT_RATIOS = dict(
    max_addresses_per_customer=3,
    max_categories_per_product=3,
    max_items_per_order=6,
    max_items_per_return=2,
    return_rate=0.10,
)
DEFAULT_DECAY = dict(addresses=0.45, categories=0.35, items=0.55, return_items=0.30)

# (country code, Faker locale, weight)
DEFAULT_LOCALES = [("GB", "en_GB", 40), ("US", "en_US", 30), ("DE", "de_DE", 8),
                   ("FR", "fr_FR", 8), ("IN", "en_IN", 8), ("AU", "en_AU", 6)]

ID_W = 10
IDX = "(id + 1)"
HOUR_W = [1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 6, 7, 8, 7, 6, 6, 6, 7, 8, 9, 9, 8, 5, 2]

ORDER_STATUS = ["Delivered", "Shipped", "Processing", "Cancelled"]
ORDER_STATUS_W = [74, 12, 6, 8]

TAXONOMY = {
    "Electronics": ["Headphones", "Laptops", "Smartphones", "Cameras", "Smart Home", "Chargers & Cables"],
    "Home & Kitchen": ["Cookware", "Small Appliances", "Bedding", "Storage", "Lighting", "Cleaning"],
    "Clothing": ["Men's Tops", "Women's Dresses", "Outerwear", "Footwear", "Activewear", "Accessories"],
    "Sports & Outdoors": ["Camping", "Cycling", "Fitness Equipment", "Running", "Water Sports", "Team Sports"],
    "Beauty & Personal Care": ["Skincare", "Haircare", "Fragrance", "Make-up", "Grooming", "Oral Care"],
    "Toys & Games": ["Board Games", "Building Sets", "Puzzles", "Outdoor Play", "Dolls & Figures", "Arts & Crafts"],
    "Books & Media": ["Fiction", "Non-Fiction", "Children's Books", "Music", "Films & TV", "Stationery"],
    "Garden & DIY": ["Power Tools", "Hand Tools", "Plants & Seeds", "Garden Furniture", "Paint", "Fixings"],
}
ADJECTIVES = ["Classic", "Premium", "Eco", "Compact", "Deluxe", "Essential", "Urban", "Smart", "Ultra",
              "Everyday", "Pro", "Lite"]
NOUNS = ["Backpack", "Kettle", "Headphones", "Desk Lamp", "Jacket", "Trainers", "Water Bottle", "Notebook",
         "Blender", "Office Chair", "Toaster", "Power Bank", "Yoga Mat", "Duvet Set", "Frying Pan", "Umbrella",
         "Wallet", "Watch", "Speaker", "Air Fryer"]
RETURN_REASONS = ["Wrong size", "Damaged on arrival", "Not as described", "No longer needed",
                  "Better price elsewhere", "Arrived late", "Defective"]
RETURN_REASON_W = [26, 18, 16, 15, 9, 8, 8]

SCHEMAS = {
    "customer": [("source_record_id", "STRING"), ("customer_id", "STRING"), ("first_name", "STRING"),
                 ("last_name", "STRING"), ("email", "STRING"), ("phone", "STRING"),
                 ("date_of_birth", "STRING"), ("loyalty_status", "STRING"), ("marketing_opt_in", "STRING"),
                 ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "address": [("source_record_id", "STRING"), ("address_id", "STRING"), ("customer_id", "STRING"),
                ("address_type", "STRING"), ("address_line_1", "STRING"), ("address_line_2", "STRING"),
                ("city", "STRING"), ("state_region", "STRING"), ("postal_code", "STRING"),
                ("country_code", "STRING"), ("is_primary", "STRING"),
                ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "product": [("source_record_id", "STRING"), ("product_id", "STRING"), ("sku", "STRING"),
                ("product_name", "STRING"), ("brand", "STRING"), ("unit_price", "DECIMAL(18,2)"),
                ("currency_code", "STRING"), ("is_active", "STRING"),
                ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "category": [("source_record_id", "STRING"), ("category_id", "STRING"), ("parent_category_id", "STRING"),
                 ("category_name", "STRING"), ("category_code", "STRING"), ("is_active", "STRING"),
                 ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "product_category": [("source_record_id", "STRING"), ("product_category_id", "STRING"),
                         ("product_id", "STRING"), ("category_id", "STRING"), ("is_primary", "STRING"),
                         ("active_from", "STRING"), ("active_to", "STRING"),
                         ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "order": [("source_record_id", "STRING"), ("order_id", "STRING"), ("customer_id", "STRING"),
              ("billing_address_id", "STRING"), ("shipping_address_id", "STRING"),
              ("order_timestamp", "TIMESTAMP"), ("channel", "STRING"), ("order_status", "STRING"),
              ("currency_code", "STRING"), ("subtotal_amount", "DECIMAL(18,2)"),
              ("discount_amount", "DECIMAL(18,2)"), ("tax_amount", "DECIMAL(18,2)"),
              ("shipping_amount", "DECIMAL(18,2)"), ("order_total_amount", "DECIMAL(18,2)"),
              ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "order_item": [("source_record_id", "STRING"), ("order_item_id", "STRING"), ("order_id", "STRING"),
                   ("product_id", "STRING"), ("quantity", "INT"), ("unit_price", "DECIMAL(18,2)"),
                   ("discount_amount", "DECIMAL(18,2)"), ("tax_amount", "DECIMAL(18,2)"),
                   ("line_total_amount", "DECIMAL(18,2)"),
                   ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "payment": [("source_record_id", "STRING"), ("payment_id", "STRING"), ("order_id", "STRING"),
                ("customer_id", "STRING"), ("payment_timestamp", "TIMESTAMP"), ("payment_method", "STRING"),
                ("payment_status", "STRING"), ("amount", "DECIMAL(18,2)"), ("currency_code", "STRING"),
                ("transaction_reference", "STRING"),
                ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "return": [("source_record_id", "STRING"), ("return_id", "STRING"), ("order_id", "STRING"),
               ("customer_id", "STRING"), ("return_timestamp", "TIMESTAMP"), ("return_status", "STRING"),
               ("return_reason", "STRING"), ("total_refund_amount", "DECIMAL(18,2)"),
               ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "return_item": [("source_record_id", "STRING"), ("return_item_id", "STRING"), ("return_id", "STRING"),
                    ("order_item_id", "STRING"), ("product_id", "STRING"), ("quantity", "INT"),
                    ("refund_amount", "DECIMAL(18,2)"), ("return_reason", "STRING"),
                    ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
}

PRIMARY_KEYS = {"customer": "customer_id", "address": "address_id", "product": "product_id",
                "category": "category_id", "product_category": "product_category_id",
                "order": "order_id", "order_item": "order_item_id", "payment": "payment_id",
                "return": "return_id", "return_item": "return_item_id"}

FOREIGN_KEYS = [
    ("address", "customer_id", "customer", "customer_id"),
    ("category", "parent_category_id", "category", "category_id"),
    ("product_category", "product_id", "product", "product_id"),
    ("product_category", "category_id", "category", "category_id"),
    ("order", "customer_id", "customer", "customer_id"),
    ("order", "billing_address_id", "address", "address_id"),
    ("order", "shipping_address_id", "address", "address_id"),
    ("order_item", "order_id", "order", "order_id"),
    ("order_item", "product_id", "product", "product_id"),
    ("payment", "order_id", "order", "order_id"),
    ("payment", "customer_id", "customer", "customer_id"),
    ("return", "order_id", "order", "order_id"),
    ("return", "customer_id", "customer", "customer_id"),
    ("return_item", "return_id", "return", "return_id"),
    ("return_item", "order_item_id", "order_item", "order_item_id"),
    ("return_item", "product_id", "product", "product_id"),
]


# =====================================================================================
# 2. CONFIG
# =====================================================================================

@dataclass(frozen=True)
class GenConfig:
    """Everything the generators need. Build it with build_config(); do not edit by hand."""
    preset: str
    seed: int
    n_cust: int
    n_prod: int
    n_cat: int
    n_ord: int
    n_ret: int
    n_roots: int
    cat_base: int
    cat_n: int
    max_addr: int
    max_pcat: int
    max_item: int
    max_ritm: int
    decay: dict
    locales: list
    brands: list
    currency: str
    data_end: str
    data_span_days: int
    lead_days: int
    ret_p: int      # multiplier linking returns to orders
    ret_pinv: int   # its modular inverse


def parse_int(text, default: int) -> int:
    """'10_000' / '10,000' / '' -> int (blank = default). Handy for widget values."""
    text = (str(text) if text is not None else "").strip().replace("_", "").replace(",", "")
    return int(text) if text else default


def make_brands(seed: int, count: int = 25) -> List[str]:
    """Reproducible Faker-generated brand pool (no quotes: dbldatagen builds SQL from these)."""
    bf = Faker("en_GB")
    bf.seed_instance(seed)
    suffix = ["", "", "", " Home", " Outdoors", " Labs", " & Co", " Studio"]
    brands: List[str] = []
    while len(brands) < count:
        b = (bf.last_name() + bf.random_element(suffix)).replace("'", "")
        if b not in brands:
            brands.append(b)
    return brands


def build_config(
    preset: str = "small",
    customers: Optional[int] = None,
    products: Optional[int] = None,
    categories: Optional[int] = None,
    orders: Optional[int] = None,
    seed: int = 42,
    ratios: Optional[dict] = None,
    decay: Optional[dict] = None,
    locales: Optional[list] = None,
    currency: str = "GBP",
    data_end: str = "2026-10-01 00:00:00",
    data_span_days: int = 730,
    lead_days: int = 30,
) -> GenConfig:
    """Resolve a preset + overrides into a fully derived GenConfig."""
    if preset not in PRESETS:
        raise ValueError(f"Unknown preset '{preset}'. Choose from {list(PRESETS)}")
    base = PRESETS[preset]
    r = {**DEFAULT_RATIOS, **(ratios or {})}
    d = {**DEFAULT_DECAY, **(decay or {})}

    n_cust = max(1, customers if customers is not None else base["customers"])
    n_prod = max(1, products if products is not None else base["products"])
    n_cat = max(1, categories if categories is not None else base["categories"])
    n_ord = max(1, orders if orders is not None else base["orders"])

    n_roots = max(1, min(8, math.ceil(n_cat / 4)))
    n_leaves = n_cat - n_roots
    cat_base = n_roots if n_leaves > 0 else 0
    cat_n = n_leaves if n_leaves > 0 else n_cat

    max_item = max(1, int(r["max_items_per_order"]))
    n_ret = 0 if r["return_rate"] <= 0 else min(n_ord, max(1, round(n_ord * r["return_rate"])))

    ret_p = next(p for p in (7919, 7907, 7901, 7883, 7877, 104729, 1299709) if math.gcd(p, n_ord) == 1)
    ret_pinv = pow(ret_p, -1, n_ord) if n_ord > 1 else 0

    return GenConfig(
        preset=preset, seed=seed,
        n_cust=n_cust, n_prod=n_prod, n_cat=n_cat, n_ord=n_ord, n_ret=n_ret,
        n_roots=n_roots, cat_base=cat_base, cat_n=cat_n,
        max_addr=max(1, int(r["max_addresses_per_customer"])),
        max_pcat=max(1, min(int(r["max_categories_per_product"]), cat_n)),
        max_item=max_item,
        max_ritm=max(1, min(int(r["max_items_per_return"]), max_item)),
        decay=d, locales=list(locales or DEFAULT_LOCALES), brands=make_brands(seed),
        currency=currency, data_end=data_end, data_span_days=data_span_days, lead_days=lead_days,
        ret_p=ret_p, ret_pinv=ret_pinv,
    )


# =====================================================================================
# 3. CATALOG HELPERS (same style as pipeline.common.catalog)
# =====================================================================================

def get_schema_fqn(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}"


def get_table_fqn(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _quote_fqn(fqn: str) -> str:
    """main.schema.order -> `main`.`schema`.`order` (order/return are SQL keywords)."""
    return ".".join(f"`{p}`" for p in fqn.split("."))


def check_table_exists(spark: SparkSession, table_fqn: str) -> bool:
    return spark.catalog.tableExists(table_fqn)


def ensure_schema_exists(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_fqn(get_schema_fqn(catalog, schema))}")


# =====================================================================================
# 4. SCHEMA HELPERS
# =====================================================================================

def ddl(table: str) -> str:
    return ", ".join(f"`{c}` {t}" for c, t in SCHEMAS[table])


def conform(df: DataFrame, table: str) -> DataFrame:
    """Select exactly the spreadsheet's columns, in order, cast to the spreadsheet's datatypes."""
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in SCHEMAS[table]])


def empty(spark: SparkSession, table: str) -> DataFrame:
    return spark.createDataFrame([], ddl(table))


# =====================================================================================
# 5. SQL-EXPRESSION HELPERS
#    Foreign keys are *computed* from row numbers, so children never need joins to parents.
# =====================================================================================

def h(cfg: GenConfig, salt: str, *cols) -> str:
    """Deterministic hash for 'random-looking but reproducible' choices."""
    return f"xxhash64({', '.join(cols)}, '{salt}', {cfg.seed})"


def fid(prefix: str, idx: str) -> str:
    """Formatted ID, e.g. CUST-0000000042"""
    return f"concat('{prefix}-', lpad(cast({idx} as string), {ID_W}, '0'))"


def src(table: str) -> str:
    return fid(f"SRC-{table.upper()}", IDX)


def slots(weights, n: int = 100) -> List[int]:
    """Weights -> integer slot counts summing to n (largest remainder)."""
    tot = float(sum(weights))
    raw = [w * n / tot for w in weights]
    cnt = [int(x) for x in raw]
    for i in sorted(range(len(raw)), key=lambda i: raw[i] - cnt[i], reverse=True)[: n - sum(cnt)]:
        cnt[i] += 1
    return cnt


def wchoose(cfg: GenConfig, salt: str, idx: str, values, weights, quote: bool = False) -> str:
    """Weighted deterministic choice from `values` (a pure function of idx)."""
    flat = [v for v, c in zip(values, slots(weights)) for _ in range(c)]
    items = ", ".join("'" + str(v).replace("'", "\\'") + "'" if quote else str(v) for v in flat)
    return f"element_at(array({items}), cast(pmod({h(cfg, salt, idx)}, {len(flat)}) + 1 as int))"


def choose(cfg: GenConfig, salt: str, idx: str, options) -> str:
    return wchoose(cfg, salt, idx, options, [1] * len(options), quote=True)


def geo_weights(max_n: int, decay: float) -> List[float]:
    return [decay ** i for i in range(max_n)]


def mean_count(max_n: int, decay: float) -> float:
    vals = list(range(1, max_n + 1))
    cnt = slots(geo_weights(max_n, decay))
    return sum(v * c for v, c in zip(vals, cnt)) / sum(cnt)


def count_sql(cfg: GenConfig, salt: str, idx: str, max_n: int, decay: float) -> str:
    """How many child rows parent `idx` has (1..max_n)."""
    return wchoose(cfg, salt, idx, list(range(1, max_n + 1)), geo_weights(max_n, decay))


def skew_pick(cfg: GenConfig, salt: str, n: int, idx: str, power: float) -> str:
    """Skewed 1..n pick: low numbers are chosen far more often."""
    return f"(cast(floor({n} * pow(pmod({h(cfg, salt, idx)}, 1000003) / 1000003.0, {power})) as bigint) + 1)"


def cust_pick(cfg: GenConfig, order_idx: str) -> str:
    return skew_pick(cfg, "ocust", cfg.n_cust, order_idx, 2.0)


def prod_pick(cfg: GenConfig, item_idx: str) -> str:
    return skew_pick(cfg, "prod", cfg.n_prod, item_idx, 2.0)


def country_sql(cfg: GenConfig, cust_idx: str) -> str:
    return wchoose(cfg, "ctry", cust_idx, [c for c, _, _ in cfg.locales], [w for _, _, w in cfg.locales], quote=True)


def price_sql(cfg: GenConfig, prod_idx: str) -> str:
    """Catalogue price: mostly cheap, long expensive tail, ends in .99."""
    u = f"(pmod({h(cfg, 'price', prod_idx)}, 1000003) / 1000003.0)"
    return f"cast(floor(4 + 196 * pow({u}, 3)) + 0.99 as decimal(18,2))"


# ---- time helpers --------------------------------------------------------------------

def end_ts(cfg: GenConfig) -> str:
    return f"timestamp'{cfg.data_end}'"


def end_lead_ts(cfg: GenConfig) -> str:
    return f"timestamp_seconds(unix_timestamp({end_ts(cfg)}) - {cfg.lead_days * 86400})"


def span_sec(cfg: GenConfig) -> int:
    return (cfg.data_span_days - cfg.lead_days) * 86400


def plus_seconds(ts_expr: str, seconds_expr) -> str:
    return f"timestamp_seconds(unix_timestamp({ts_expr}) + {seconds_expr})"


def rand_ts(cfg: GenConfig, salt: str, idx: str) -> str:
    """Uniform timestamp in [data_end - span, data_end - lead_days]."""
    return (f"timestamp_seconds(unix_timestamp({end_ts(cfg)}) - {cfg.lead_days * 86400} "
            f"- pmod({h(cfg, salt, idx)}, {span_sec(cfg)}))")


def updated_ts(cfg: GenConfig, created_col: str, salt: str) -> str:
    """updated_at = created_at + 0..30 days (never after data_end)."""
    offset = f"pmod({h(cfg, salt, 'id')}, 2592000)"
    return f"least({plus_seconds(created_col, offset)}, {end_ts(cfg)})"


def cust_created(cfg: GenConfig, cust_idx: str) -> str:
    return rand_ts(cfg, "cust_cr", cust_idx)


def peak_dates(cfg: GenConfig) -> List[date]:
    """Black-Friday / Christmas windows (20 Nov - 24 Dec) fully inside the data range."""
    end = datetime.strptime(cfg.data_end, "%Y-%m-%d %H:%M:%S")
    start = end - timedelta(days=cfg.data_span_days)
    return [date(y, 11, 20) for y in range(start.year, end.year + 1)
            if datetime(y, 11, 20) >= start and datetime(y, 12, 24) <= end - timedelta(days=cfg.lead_days)]


def order_ts(cfg: GenConfig, order_idx: str) -> str:
    """Evening-heavy, ~22% in the Nov/Dec peak, never before the customer signed up."""
    uni_day = (f"date_sub(to_date({end_ts(cfg)}), cast({cfg.lead_days} + "
               f"pmod({h(cfg, 'od', order_idx)}, {cfg.data_span_days - cfg.lead_days}) as int))")
    peaks = peak_dates(cfg)
    if peaks:
        wins = ", ".join(f"to_date('{p.isoformat()}')" for p in peaks)
        peak_day = (f"date_add(element_at(array({wins}), cast(pmod({h(cfg, 'pw', order_idx)}, {len(peaks)}) + 1 as int)), "
                    f"cast(pmod({h(cfg, 'pd', order_idx)}, 35) as int))")
        day = f"case when pmod({h(cfg, 'pk', order_idx)}, 100) < 22 then {peak_day} else {uni_day} end"
    else:
        day = uni_day
    secs = (f"({wchoose(cfg, 'hr', order_idx, list(range(24)), HOUR_W)}) * 3600 "
            f"+ pmod({h(cfg, 'hs', order_idx)}, 3600)")
    raw = f"timestamp_seconds(unix_timestamp(cast({day} as timestamp)) + {secs})"
    earliest = plus_seconds(cust_created(cfg, cust_pick(cfg, order_idx)), f"pmod({h(cfg, 'oc', order_idx)}, 604800)")
    return f"least(greatest({raw}, {earliest}), {end_lead_ts(cfg)})"


# ---- order <-> return linkage --------------------------------------------------------
# Return r refers to order ((r-1)*P mod N_ORD)+1; P is coprime with N_ORD so every return
# hits a different order, and PINV answers "is this order returned?" without a join.

def ret_order(cfg: GenConfig, ridx: str) -> str:
    return f"(pmod(({ridx} - 1) * {cfg.ret_p}, {cfg.n_ord}) + 1)"


def is_returned(cfg: GenConfig, oidx: str) -> str:
    return f"(pmod(({oidx} - 1) * {cfg.ret_pinv}, {cfg.n_ord}) < {cfg.n_ret})"


def order_status_sql(cfg: GenConfig, oidx: str) -> str:
    return (f"case when {is_returned(cfg, oidx)} then 'Delivered' "
            f"else {wchoose(cfg, 'ostat', oidx, ORDER_STATUS, ORDER_STATUS_W, quote=True)} end")


def ret_ts(cfg: GenConfig, ridx: str) -> str:
    """3..28 days after the order."""
    return plus_seconds(order_ts(cfg, ret_order(cfg, ridx)),
                        f"86400 * 3 + pmod({h(cfg, 'rts', ridx)}, {25 * 86400})")


def ret_reason(cfg: GenConfig, ridx: str) -> str:
    return wchoose(cfg, "rr", ridx, RETURN_REASONS, RETURN_REASON_W, quote=True)


def make_gen(spark: SparkSession, cfg: GenConfig, name: str, rows: int) -> dg.DataGenerator:
    return dg.DataGenerator(spark, name=name, rows=rows, partitions=max(1, rows // 200_000),
                            randomSeed=cfg.seed, randomSeedMethod="hash_fieldname")


# =====================================================================================
# 6. FAKER UDFs (per-row seeded => reproducible regardless of partitioning)
# =====================================================================================

def build_faker_udfs(seed: int, locale_by_country: Dict[str, str]):
    """Returns (fake_person, fake_address) pandas UDFs."""
    cache = {}

    def fk(country):
        f = cache.get(country)
        if f is None:
            from faker import Faker as _Faker
            f = cache[country] = _Faker(locale_by_country.get(country, "en_GB"))
        return f

    def ascii_alnum(s):
        import re as _re, unicodedata as _ud
        return _re.sub(r"[^a-z0-9]", "", _ud.normalize("NFKD", s).encode("ascii", "ignore").decode().lower())

    FALLBACK_LINE2 = {"DE": "Wohnung {n}", "FR": "Appt {n}", "IN": "Flat {n}"}
    GLOBAL_DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "icloud.com", "proton.me", "aol.com"]
    GLOBAL_W = [46, 14, 12, 10, 10, 4, 4]
    LOCAL_DOMAINS = {"GB": ["btinternet.com", "sky.com", "hotmail.co.uk", "yahoo.co.uk"],
                     "DE": ["web.de", "gmx.de", "t-online.de"], "FR": ["orange.fr", "free.fr", "laposte.net"],
                     "IN": ["rediffmail.com"], "AU": ["bigpond.com", "optusnet.com.au"],
                     "US": ["comcast.net", "att.net"]}

    @pandas_udf("first_name string, last_name string, email string, phone string, date_of_birth string")
    def fake_person(idx: pd.Series, country: pd.Series) -> pd.DataFrame:
        rows = []
        for i, c in zip(idx, country):
            f = fk(c)
            f.seed_instance(seed * 1_000_003 + int(i))
            r = f.random
            first, last = f.first_name(), f.last_name()
            fn, ln = ascii_alnum(first) or "user", ascii_alnum(last) or "customer"
            p = r.random()
            if p < 0.45:
                local = f"{fn}.{ln}"
            elif p < 0.65:
                local = f"{fn}{ln}"
            elif p < 0.80:
                local = f"{fn[0]}{ln}"
            elif p < 0.92:
                local = f"{fn}.{ln}{r.randint(1, 99)}"
            else:
                local = f"{fn}{r.randint(1970, 2005)}"
            if c in LOCAL_DOMAINS and r.random() < 0.2:
                domain = r.choice(LOCAL_DOMAINS[c])
            else:
                domain = r.choices(GLOBAL_DOMAINS, GLOBAL_W)[0]
            email = None if r.random() < 0.03 else f"{local}@{domain}"
            phone = None if r.random() < 0.08 else f.phone_number()
            dob = None if r.random() < 0.04 else f.date_of_birth(minimum_age=18, maximum_age=85).isoformat()
            rows.append((first, last, email, phone, dob))
        return pd.DataFrame(rows, columns=["first_name", "last_name", "email", "phone", "date_of_birth"])

    @pandas_udf("address_line_1 string, address_line_2 string, city string, state_region string, postal_code string")
    def fake_address(idx: pd.Series, country: pd.Series) -> pd.DataFrame:
        rows = []
        for i, c in zip(idx, country):
            f = fk(c)
            f.seed_instance(seed * 1_000_033 + int(i))
            r = f.random
            line1 = f.street_address().replace("\n", ", ")
            if c == "FR" and not line1[:1].isdigit():
                line1 = f"{r.randint(1, 120)} {line1}"
            line2 = None
            if r.random() < 0.2:
                try:
                    line2 = f.secondary_address().strip()
                except AttributeError:
                    line2 = FALLBACK_LINE2.get(c, "Unit {n}").format(n=r.randint(1, 60))
            try:
                state = f.administrative_unit()
            except Exception:
                state = None
            rows.append((line1, line2, f.city(), state, f.postcode()))
        return pd.DataFrame(rows, columns=["address_line_1", "address_line_2", "city", "state_region", "postal_code"])

    return fake_person, fake_address


def get_faker_udfs(cfg: GenConfig):
    return build_faker_udfs(cfg.seed, {c: loc for c, loc, _ in cfg.locales})


# =====================================================================================
# 7. TABLE GENERATORS (one function per table)
# =====================================================================================

def generate_customers(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    fake_person, _ = get_faker_udfs(cfg)
    df = (
        make_gen(spark, cfg, "customer", cfg.n_cust)
        .withIdOutput()
        .withColumn("source_record_id", "string", expr=src("customer"))
        .withColumn("customer_id", "string", expr=fid("CUST", IDX))
        .withColumn("_country", "string", expr=country_sql(cfg, IDX))
        .withColumn("loyalty_status", "string", values=["Bronze", "Silver", "Gold", "Platinum"],
                    weights=[55, 28, 13, 4], random=True)
        .withColumn("marketing_opt_in", "string", values=["Y", "N"], weights=[6, 4], random=True)
        .withColumn("created_at", "timestamp", expr=cust_created(cfg, IDX))
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "cust_up"))
        .build()
    )
    df = (df.withColumn("_p", fake_person(F.col("id"), F.col("_country")))
            .select("*", "_p.first_name", "_p.last_name", "_p.email", "_p.phone", "_p.date_of_birth"))
    return conform(df, "customer")


def generate_addresses(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    """Customers have 1..max_addr addresses; slot 0 is the primary/billing address."""
    _, fake_address = get_faker_udfs(cfg)
    a_cust = f"(pmod(id, {cfg.n_cust}) + 1)"
    a_k = f"(id div {cfg.n_cust})"
    df = (
        make_gen(spark, cfg, "address", cfg.n_cust * cfg.max_addr)
        .withIdOutput()
        .withColumn("source_record_id", "string", expr=src("address"))
        .withColumn("address_id", "string", expr=fid("ADDR", IDX))
        .withColumn("customer_id", "string", expr=fid("CUST", a_cust))
        .withColumn("_keep", "boolean",
                    expr=f"{a_k} < {count_sql(cfg, 'naddr', a_cust, cfg.max_addr, cfg.decay['addresses'])}")
        .withColumn("_country", "string", expr=country_sql(cfg, a_cust))
        .withColumn("country_code", "string", baseColumn="_country", expr="_country")
        .withColumn("address_type", "string", expr=f"case when {a_k} = 0 then 'BILLING' else 'SHIPPING' end")
        .withColumn("is_primary", "string", expr=f"case when {a_k} = 0 then 'Y' else 'N' end")
        .withColumn("created_at", "timestamp",
                    expr=f"least({plus_seconds(cust_created(cfg, a_cust), 'pmod(%s, 259200)' % h(cfg, 'ac', 'id'))}, {end_ts(cfg)})")
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "addr_up"))
        .build()
        .where("_keep")
    )
    df = (df.withColumn("_a", fake_address(F.col("id"), F.col("_country")))
            .select("*", "_a.address_line_1", "_a.address_line_2", "_a.city", "_a.state_region", "_a.postal_code"))
    return conform(df, "address")


def generate_products(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    brand_w = [1.0 / (i + 1) for i in range(len(cfg.brands))]
    df = (
        make_gen(spark, cfg, "product", cfg.n_prod)
        .withColumn("source_record_id", "string", expr=src("product"))
        .withColumn("product_id", "string", expr=fid("PROD", IDX))
        .withColumn("sku", "string", expr=f"concat('SKU-', lpad(cast({IDX} as string), 7, '0'))")
        .withColumn("brand", "string", values=cfg.brands, weights=brand_w, random=True)
        .withColumn("product_name", "string", baseColumn="brand",
                    expr=(f"concat(brand, ' ', {choose(cfg, 'adj', 'id', ADJECTIVES)}, ' ', {choose(cfg, 'noun', 'id', NOUNS)}, "
                          f"case when pmod({h(cfg, 'mdl', 'id')}, 10) < 3 "
                          f"then concat(' ', char(65 + cast(pmod({h(cfg, 'ml', 'id')}, 26) as int)), cast(100 + pmod({h(cfg, 'mn', 'id')}, 900) as string)) "
                          f"else '' end)"))
        .withColumn("unit_price", "decimal(18,2)", expr=price_sql(cfg, IDX))
        .withColumn("currency_code", "string", expr=f"'{cfg.currency}'")
        .withColumn("is_active", "string", values=["Y", "N"], weights=[93, 7], random=True)
        .withColumn("created_at", "timestamp", expr=rand_ts(cfg, "prod_cr", IDX))
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "prod_up"))
        .build()
    )
    return conform(df, "product")


def category_name_for_index(cfg: GenConfig, i: int) -> str:
    """Name of category row i (0-based): departments first, then leaves round-robin under them."""
    depts = list(TAXONOMY)
    if i < cfg.n_roots:
        return depts[i] if i < len(depts) else f"Department {i + 1}"
    j = i - cfg.n_roots
    dept = category_name_for_index(cfg, j % cfg.n_roots)
    subs = TAXONOMY.get(dept, [])
    return subs[j // cfg.n_roots] if j // cfg.n_roots < len(subs) else f"{dept} - Range {j // cfg.n_roots + 1}"


def category_name_expr(cfg: GenConfig) -> str:
    if cfg.n_cat <= 500:
        names = ", ".join("'" + category_name_for_index(cfg, i).replace("'", "\\'") + "'" for i in range(cfg.n_cat))
        return f"element_at(array({names}), cast(id + 1 as int))"
    return f"case when id < {cfg.n_roots} then concat('Department ', {IDX}) else concat('Category ', {IDX}) end"


def generate_categories(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    cat_parent = f"(pmod(id - {cfg.n_roots}, {cfg.n_roots}) + 1)"
    df = (
        make_gen(spark, cfg, "category", cfg.n_cat)
        .withColumn("source_record_id", "string", expr=src("category"))
        .withColumn("category_id", "string", expr=fid("CAT", IDX))
        .withColumn("parent_category_id", "string",
                    expr=f"case when id < {cfg.n_roots} then cast(null as string) else {fid('CAT', cat_parent)} end")
        .withColumn("category_name", "string", expr=category_name_expr(cfg))
        .withColumn("category_code", "string", expr=f"concat('CAT', lpad(cast({IDX} as string), 4, '0'))")
        .withColumn("is_active", "string", values=["Y", "N"], weights=[96, 4], random=True)
        .withColumn("created_at", "timestamp", expr=rand_ts(cfg, "cat_cr", IDX))
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "cat_up"))
        .build()
    )
    return conform(df, "category")


def generate_product_categories(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    """Each product sits in 1..max_pcat distinct leaf categories; slot 0 is primary."""
    pc_prod = f"(pmod(id, {cfg.n_prod}) + 1)"
    pc_k = f"(id div {cfg.n_prod})"
    pc_first = f"(pmod({h(cfg, 'pcat', pc_prod)}, {cfg.cat_n}) + 1)"
    pc_cat = f"({cfg.cat_base} + pmod({pc_first} - 1 + {pc_k}, {cfg.cat_n}) + 1)"
    span = cfg.data_span_days - cfg.lead_days
    df = (
        make_gen(spark, cfg, "product_category", cfg.n_prod * cfg.max_pcat)
        .withColumn("source_record_id", "string", expr=src("product_category"))
        .withColumn("product_category_id", "string", expr=fid("PC", IDX))
        .withColumn("product_id", "string", expr=fid("PROD", pc_prod))
        .withColumn("category_id", "string", expr=fid("CAT", pc_cat))
        .withColumn("_keep", "boolean",
                    expr=f"{pc_k} < {count_sql(cfg, 'npcat', pc_prod, cfg.max_pcat, cfg.decay['categories'])}")
        .withColumn("is_primary", "string", expr=f"case when {pc_k} = 0 then 'Y' else 'N' end")
        .withColumn("active_from", "string",
                    expr=f"date_format(date_sub(to_date({end_ts(cfg)}), cast({cfg.lead_days} + pmod({h(cfg, 'af', 'id')}, {span}) as int)), 'yyyy-MM-dd')")
        .withColumn("active_to", "string", baseColumn="active_from",
                    expr=f"case when pmod({h(cfg, 'at', 'id')}, 10) = 0 then date_format(date_add(to_date(active_from), 180), 'yyyy-MM-dd') else cast(null as string) end")
        .withColumn("created_at", "timestamp", expr=rand_ts(cfg, "pc_cr", IDX))
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "pc_up"))
        .build()
        .where("_keep")
    )
    return conform(df, "product_category")


def generate_order_items(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    """Every order has 1..max_item lines."""
    oi_order = f"(id div {cfg.max_item} + 1)"
    oi_k = f"pmod(id, {cfg.max_item})"
    oi_prod = prod_pick(cfg, IDX)
    disc = wchoose(cfg, "disc", "id", [0.0, 0.05, 0.10, 0.15, 0.20, 0.30], [66, 10, 12, 6, 4, 2])
    df = (
        make_gen(spark, cfg, "order_item", cfg.n_ord * cfg.max_item)
        .withColumn("source_record_id", "string", expr=src("order_item"))
        .withColumn("order_item_id", "string", expr=fid("OITEM", IDX))
        .withColumn("order_id", "string", expr=fid("ORD", oi_order))
        .withColumn("product_id", "string", expr=fid("PROD", oi_prod))
        .withColumn("_keep", "boolean",
                    expr=f"{oi_k} < {count_sql(cfg, 'lines', oi_order, cfg.max_item, cfg.decay['items'])}")
        .withColumn("quantity", "int", expr=wchoose(cfg, "qty", "id", [1, 2, 3, 4, 5], [68, 20, 7, 3, 2]))
        .withColumn("unit_price", "decimal(18,2)", expr=price_sql(cfg, oi_prod))
        .withColumn("discount_amount", "decimal(18,2)", baseColumn=["quantity", "unit_price"],
                    expr=f"round(quantity * unit_price * {disc}, 2)")
        .withColumn("tax_amount", "decimal(18,2)", baseColumn=["quantity", "unit_price", "discount_amount"],
                    expr="round((quantity * unit_price - discount_amount) * 0.20, 2)")
        .withColumn("line_total_amount", "decimal(18,2)",
                    baseColumn=["quantity", "unit_price", "discount_amount", "tax_amount"],
                    expr="quantity * unit_price - discount_amount + tax_amount")
        .withColumn("created_at", "timestamp", expr=order_ts(cfg, oi_order))
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "oi_up"))
        .build()
        .where("_keep")
    )
    return conform(df, "order_item")


def generate_orders(spark: SparkSession, cfg: GenConfig, order_item: DataFrame) -> DataFrame:
    """Money columns are rolled up from order_item so the books balance."""
    o_cust = cust_pick(cfg, IDX)
    o_ship = (f"({o_cust} + pmod({h(cfg, 'ship', IDX)}, "
              f"{count_sql(cfg, 'naddr', o_cust, cfg.max_addr, cfg.decay['addresses'])}) * {cfg.n_cust})")
    base = (
        make_gen(spark, cfg, "order", cfg.n_ord)
        .withColumn("source_record_id", "string", expr=src("order"))
        .withColumn("order_id", "string", expr=fid("ORD", IDX))
        .withColumn("customer_id", "string", expr=fid("CUST", o_cust))
        .withColumn("billing_address_id", "string", expr=fid("ADDR", o_cust))
        .withColumn("shipping_address_id", "string", expr=fid("ADDR", o_ship))
        .withColumn("order_timestamp", "timestamp", expr=order_ts(cfg, IDX))
        .withColumn("channel", "string", values=["Web", "Mobile App", "Store", "Call Centre", "Marketplace"],
                    weights=[46, 30, 12, 4, 8], random=True)
        .withColumn("order_status", "string", expr=order_status_sql(cfg, IDX))
        .withColumn("currency_code", "string", expr=f"'{cfg.currency}'")
        .withColumn("created_at", "timestamp", baseColumn="order_timestamp", expr="order_timestamp")
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "ord_up"))
        .build()
    )
    totals = (order_item.groupBy("order_id")
              .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("_subtotal"),
                   F.sum("discount_amount").alias("_discount"),
                   F.sum("tax_amount").alias("_tax")))
    df = (base.join(totals, "order_id", "left")
          .withColumn("subtotal_amount", F.coalesce("_subtotal", F.lit(0)))
          .withColumn("discount_amount", F.coalesce("_discount", F.lit(0)))
          .withColumn("tax_amount", F.coalesce("_tax", F.lit(0)))
          .withColumn("shipping_amount", F.when(F.col("subtotal_amount") >= 50, F.lit(0)).otherwise(F.lit(4.99)))
          .withColumn("order_total_amount",
                      F.col("subtotal_amount") - F.col("discount_amount") + F.col("tax_amount") + F.col("shipping_amount")))
    return conform(df, "order")


def generate_payments(spark: SparkSession, cfg: GenConfig, order: DataFrame) -> DataFrame:
    """One per order; amount = order total; status follows the order's status."""
    sha = lambda n: f"substring(sha2(concat(cast(id as string), '{cfg.seed}'), 256), 1, {n})"
    base = (
        make_gen(spark, cfg, "payment", cfg.n_ord)
        .withColumn("source_record_id", "string", expr=src("payment"))
        .withColumn("payment_id", "string", expr=fid("PAY", IDX))
        .withColumn("order_id", "string", expr=fid("ORD", IDX))
        .withColumn("_ostat", "string", expr=order_status_sql(cfg, IDX))
        .withColumn("payment_timestamp", "timestamp",
                    expr=plus_seconds(order_ts(cfg, IDX), f"pmod({h(cfg, 'payts', 'id')}, 900)"))
        .withColumn("payment_method", "string",
                    values=["Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer", "Gift Card"],
                    weights=[55, 22, 12, 5, 4, 2], random=True)
        .withColumn("payment_status", "string", baseColumn="_ostat",
                    expr=(f"case when _ostat = 'Cancelled' then {wchoose(cfg, 'pst_c', 'id', ['Failed', 'Voided', 'Refunded'], [50, 30, 20], quote=True)} "
                          f"when _ostat = 'Processing' then 'Authorised' "
                          f"when {is_returned(cfg, IDX)} then {wchoose(cfg, 'pst_r', 'id', ['Captured', 'Partially Refunded', 'Refunded'], [20, 50, 30], quote=True)} "
                          f"else 'Captured' end"))
        .withColumn("transaction_reference", "string", baseColumn="payment_method",
                    expr=(f"case payment_method "
                          f"when 'Card' then concat('ch_', lower({sha(24)})) "
                          f"when 'PayPal' then concat('PAYID-', upper({sha(20)})) "
                          f"else concat('TXN-', upper({sha(16)})) end"))
        .withColumn("created_at", "timestamp", baseColumn="payment_timestamp", expr="payment_timestamp")
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "pay_up"))
        .build()
    )
    df = base.join(order.select("order_id", "customer_id",
                                F.col("order_total_amount").alias("amount"), "currency_code"),
                   "order_id", "left")
    return conform(df, "payment")


def generate_returns(spark: SparkSession, cfg: GenConfig, order_item: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Only delivered orders are returned; refunds come from the returned lines.
    Returns (return_df, return_item_df)."""
    if cfg.n_ret == 0:
        return empty(spark, "return"), empty(spark, "return_item")

    ret_oidx = ret_order(cfg, IDX)
    return_base = (
        make_gen(spark, cfg, "return", cfg.n_ret)
        .withColumn("source_record_id", "string", expr=src("return"))
        .withColumn("return_id", "string", expr=fid("RET", IDX))
        .withColumn("order_id", "string", expr=fid("ORD", ret_oidx))
        .withColumn("customer_id", "string", expr=fid("CUST", cust_pick(cfg, ret_oidx)))
        .withColumn("return_timestamp", "timestamp", expr=ret_ts(cfg, IDX))
        .withColumn("return_status", "string", values=["Requested", "Approved", "Refunded", "Rejected"],
                    weights=[10, 15, 68, 7], random=True)
        .withColumn("return_reason", "string", expr=ret_reason(cfg, IDX))
        .withColumn("created_at", "timestamp", baseColumn="return_timestamp", expr="return_timestamp")
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "ret_up"))
        .build()
    )

    ri_ret = f"(id div {cfg.max_ritm} + 1)"
    ri_k = f"pmod(id, {cfg.max_ritm})"
    ri_ord = ret_order(cfg, ri_ret)
    ri_oi = f"(({ri_ord} - 1) * {cfg.max_item} + {ri_k} + 1)"   # order_item number being returned
    ri_cnt = (f"least({cfg.max_ritm}, {count_sql(cfg, 'lines', ri_ord, cfg.max_item, cfg.decay['items'])}, "
              f"{count_sql(cfg, 'rlines', ri_ret, cfg.max_ritm, cfg.decay['return_items'])})")
    ri_base = (
        make_gen(spark, cfg, "return_item", cfg.n_ret * cfg.max_ritm)
        .withColumn("source_record_id", "string", expr=src("return_item"))
        .withColumn("return_item_id", "string", expr=fid("RI", IDX))
        .withColumn("return_id", "string", expr=fid("RET", ri_ret))
        .withColumn("order_item_id", "string", expr=fid("OITEM", ri_oi))
        .withColumn("product_id", "string", expr=fid("PROD", prod_pick(cfg, ri_oi)))
        .withColumn("_keep", "boolean", expr=f"{ri_k} < {ri_cnt}")
        .withColumn("return_reason", "string", expr=ret_reason(cfg, ri_ret))
        .withColumn("created_at", "timestamp", expr=ret_ts(cfg, ri_ret))
        .withColumn("updated_at", "timestamp", baseColumn="created_at",
                    expr=updated_ts(cfg, "created_at", "ri_up"))
        .build()
        .where("_keep")
    )
    oi_lookup = order_item.select("order_item_id", F.col("quantity").alias("_oi_qty"),
                                  F.col("line_total_amount").alias("_oi_total"))
    return_item = (
        ri_base.join(oi_lookup, "order_item_id", "left")
        .withColumn("quantity", F.expr(
            f"cast(case when pmod({h(cfg, 'rq', 'return_item_id')}, 10) < 7 then _oi_qty "
            f"else 1 + pmod({h(cfg, 'rq2', 'return_item_id')}, _oi_qty) end as int)"))
        .withColumn("refund_amount", F.round(F.col("quantity") / F.col("_oi_qty") * F.col("_oi_total"), 2))
    )
    return_item = conform(return_item, "return_item")
    ret_totals = return_item.groupBy("return_id").agg(F.sum("refund_amount").alias("total_refund_amount"))
    return_ = conform(return_base.join(ret_totals, "return_id", "left"), "return")
    return return_, return_item


def generate_all_tables(spark: SparkSession, cfg: GenConfig) -> Dict[str, DataFrame]:
    """Generate all 10 tables in dependency order. Returns {table_name: DataFrame}."""
    customer = generate_customers(spark, cfg)
    address = generate_addresses(spark, cfg)
    product = generate_products(spark, cfg)
    category = generate_categories(spark, cfg)
    product_category = generate_product_categories(spark, cfg)
    order_item = generate_order_items(spark, cfg)
    order = generate_orders(spark, cfg, order_item)
    payment = generate_payments(spark, cfg, order)
    return_, return_item = generate_returns(spark, cfg, order_item)
    return {
        "customer": customer, "address": address, "product": product, "category": category,
        "product_category": product_category, "order": order, "order_item": order_item,
        "payment": payment, "return": return_, "return_item": return_item,
    }


# =====================================================================================
# 8. PLAN, PUBLISH, CHECKS
# =====================================================================================

def get_row_plan(cfg: GenConfig) -> Dict[str, Tuple[float, bool]]:
    """{table: (expected rows, is_exact)}"""
    d = cfg.decay
    return {
        "customer": (cfg.n_cust, True),
        "address": (cfg.n_cust * mean_count(cfg.max_addr, d["addresses"]), False),
        "product": (cfg.n_prod, True),
        "category": (cfg.n_cat, True),
        "product_category": (cfg.n_prod * mean_count(cfg.max_pcat, d["categories"]), False),
        "order": (cfg.n_ord, True),
        "order_item": (cfg.n_ord * mean_count(cfg.max_item, d["items"]), False),
        "payment": (cfg.n_ord, True),
        "return": (cfg.n_ret, True),
        "return_item": (cfg.n_ret * min(mean_count(cfg.max_ritm, d["return_items"]),
                                        mean_count(cfg.max_item, d["items"])), False),
    }


def print_row_plan(cfg: GenConfig, output_mode: str = "") -> None:
    print(f"Preset = {cfg.preset}  (seed {cfg.seed}, output = {output_mode})   "
          f"[~ = approximate: child counts vary per parent]")
    for t, (n, exact) in get_row_plan(cfg).items():
        print(f"  {t:<17}{'' if exact else '~'}{n:>13,.0f}")


def publish_tables(spark: SparkSession, tables: Dict[str, DataFrame], output_mode: str = "temp_views",
                   catalog: str = "main", schema: str = "synthetic_ecommerce") -> Dict[str, DataFrame]:
    """temp_views: register views. delta: write tables and return read-back DataFrames."""
    if output_mode == "delta":
        ensure_schema_exists(spark, catalog, schema)
        out = {}
        for name, df in tables.items():
            fqn = get_table_fqn(catalog, schema, name)
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_quote_fqn(fqn))
            out[name] = spark.table(_quote_fqn(fqn))   # read back so later steps don't regenerate
            print(f"wrote {fqn}")
        return out
    if output_mode != "temp_views":
        raise ValueError("output_mode must be 'temp_views' or 'delta'")
    for name, df in tables.items():
        df.createOrReplaceTempView(name)   # `order` and `return` are SQL keywords: query with backticks
    print("Temp views created:", ", ".join(tables))
    return tables


def show(df: DataFrame, n: int = 10) -> None:
    try:
        display(df.limit(n))   # noqa: F821  (Databricks builtin)
    except NameError:
        df.show(n, truncate=False)


def run_checks(tables: Dict[str, DataFrame], cfg: GenConfig, raise_on_fail: bool = True) -> List[str]:
    """PK uniqueness, row counts, FK orphans and business rules. Returns the list of problems."""
    T = tables
    problems: List[str] = []
    plan = get_row_plan(cfg)

    for t, df in T.items():
        n = df.count()
        dupes = n - df.select(PRIMARY_KEYS[t]).distinct().count()
        expected, exact = plan[t]
        bad = dupes > 0 or (exact and n != expected)
        if bad:
            problems.append(t)
        print(f"{'PROBLEM' if bad else 'OK':<8}{t:<18}rows={n:>12,} "
              f"(expected {'' if exact else '~'}{expected:,.0f})  duplicate PKs={dupes}")

    def orphans(child, col, parent, pcol):
        return (T[child].where(F.col(col).isNotNull())
                .join(T[parent].select(F.col(pcol).alias("_k")), F.col(col) == F.col("_k"), "left_anti").count())

    for ct, cc, pt, pc in FOREIGN_KEYS:
        o = orphans(ct, cc, pt, pc)
        if o:
            problems.append(f"{ct}.{cc}")
        print(f"{'OK' if o == 0 else 'ORPHANS':<8}{ct}.{cc} -> {pt}.{pc}  orphan rows={o}")

    def rule(name, bad_count):
        if bad_count:
            problems.append(name)
        print(f"{'OK' if bad_count == 0 else 'PROBLEM':<8}{name} (violations = {bad_count})")

    rule("order totals add up", T["order"].where(
        F.col("order_total_amount") != F.col("subtotal_amount") - F.col("discount_amount")
        + F.col("tax_amount") + F.col("shipping_amount")).count())
    rule("every customer has a primary address",
         T["customer"].join(T["address"].where("is_primary = 'Y'").select("customer_id"),
                            "customer_id", "left_anti").count())
    rule("every order has at least one item",
         T["order"].join(T["order_item"].select("order_id").distinct(), "order_id", "left_anti").count())
    rule("every return has at least one item",
         T["return"].join(T["return_item"].select("return_id").distinct(), "return_id", "left_anti").count())
    rule("every product has a primary category",
         T["product"].join(T["product_category"].where("is_primary = 'Y'").select("product_id"),
                           "product_id", "left_anti").count())
    rule("orders placed after customer signed up",
         T["order"].join(T["customer"].select("customer_id", F.col("created_at").alias("_cc")), "customer_id")
         .where(F.col("order_timestamp") < F.col("_cc")).count())
    rule("shipping address belongs to the order's customer",
         T["order"].join(T["address"].select(F.col("address_id").alias("shipping_address_id"),
                                             F.col("customer_id").alias("_ac")), "shipping_address_id")
         .where(F.col("customer_id") != F.col("_ac")).count())
    rule("payments are after their order",
         T["payment"].join(T["order"].select("order_id", "order_timestamp"), "order_id")
         .where(F.col("payment_timestamp") < F.col("order_timestamp")).count())
    rule("returns are after their order and only on delivered orders",
         T["return"].join(T["order"].select("order_id", "order_timestamp", "order_status"), "order_id")
         .where((F.col("return_timestamp") <= F.col("order_timestamp"))
                | (F.col("order_status") != "Delivered")).count())
    rule("returned quantity never exceeds ordered quantity",
         T["return_item"].join(T["order_item"].select("order_item_id", F.col("quantity").alias("_q")),
                               "order_item_id")
         .where(F.col("quantity") > F.col("_q")).count())

    if raise_on_fail and problems:
        raise AssertionError(f"Checks failed: {problems}")
    if not problems:
        print("\nAll checks passed.")
    return problems