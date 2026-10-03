import json
import os
import statistics
import sys
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# workers need the same python as the driver
os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("capstone")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "../data"
OUT_DIR = "out"

# setup
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("ts", StringType(), True),
    StructField("country", StringType(), True),
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
])

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
])

raw_orders = spark.read.csv(f"{DATA_DIR}/orders.csv", header=True, schema=orders_schema)
raw_customers = spark.read.csv(f"{DATA_DIR}/customers.csv", header=True, schema=customers_schema)
products = spark.read.csv(f"{DATA_DIR}/products.csv", header=True, schema=products_schema)

acct = {}
acct["orders_start"] = raw_orders.count()
acct["customers_start"] = raw_customers.count()
acct["products_start"] = products.count()

# duplicate orders
# order_id is the key of an order, so a repeat means the same order was written twice.
# keying on order_id alone also catches copies that differ in some other column.
deduped_orders = raw_orders.dropDuplicates(["order_id"])
acct["orders_after_dedup"] = deduped_orders.count()
acct["duplicates_removed"] = acct["orders_start"] - acct["orders_after_dedup"]

# bad amounts
# empty cells are read as null, so missing and null are the same thing here.
bad_amount_count = deduped_orders.filter(F.col("amount").isNull() | (F.col("amount") <= 0)).count()
acct["bad_amount_removed"] = bad_amount_count
after_amount_filter = deduped_orders.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
acct["orders_after_amount_filter"] = after_amount_filter.count()

# bad timestamps
# plain to_timestamp fails on "not-a-date" in ansi mode, try_to_timestamp gives null instead.
parsed_ts = after_amount_filter.withColumn(
    "ts_parsed", F.try_to_timestamp(F.col("ts"), F.lit("yyyy-MM-dd HH:mm:ss"))
)
bad_ts_count = parsed_ts.filter(F.col("ts_parsed").isNull()).count()
acct["bad_timestamp_removed"] = bad_ts_count
clean_ts = parsed_ts.filter(F.col("ts_parsed").isNotNull()).drop("ts").withColumnRenamed("ts_parsed", "ts")
acct["orders_after_ts_filter"] = clean_ts.count()
orders_pre_orphan_check = clean_ts  # orphans still in here

# duplicate customers
# window function: rank by signup_date desc per customer and keep the first.
w = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc())
clean_customers = (
    raw_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
acct["customer_dupes_removed"] = raw_customers.count() - clean_customers.count()
acct["clean_customers_count"] = clean_customers.count()

# orphan orders
# left_anti keeps orders whose customer_id has no match in customers.
# broadcast hint: customers are tiny, and the hint survives the part c settings.
orphan_orders = orders_pre_orphan_check.join(F.broadcast(clean_customers), on="customer_id", how="left_anti")
acct["orphans_set_aside"] = orphan_orders.count()

# the two clean dataframes used from here on
clean_orders = orders_pre_orphan_check.join(
    F.broadcast(clean_customers.select("customer_id")), on="customer_id", how="left_semi"
)
acct["clean_rows_remaining"] = clean_orders.count()

# coalesce(1) is fine here, only ~10k rows.
orphan_orders.coalesce(1).write.mode("overwrite").csv(f"{OUT_DIR}/orphan_orders", header=True)

# accounting report

print("\n===== DATA QUALITY REPORT (Part A) =====")
print(f"Orders at start:            {acct['orders_start']:>10,}")
print(f"Duplicates removed:         {acct['duplicates_removed']:>10,}")
print(f"Bad amounts removed:        {acct['bad_amount_removed']:>10,}")
print(f"Bad timestamps removed:     {acct['bad_timestamp_removed']:>10,}")
print(f"Orphans set aside:          {acct['orphans_set_aside']:>10,}")
print(f"Clean orders remaining:     {acct['clean_rows_remaining']:>10,}")
print(f"Customers at start:         {acct['customers_start']:>10,}")
print(f"Customer dupes removed:     {acct['customer_dupes_removed']:>10,}")
print(f"Clean customers remaining:  {acct['clean_customers_count']:>10,}")
print("========================================\n")

assert acct["orders_start"] - acct["duplicates_removed"] - acct["bad_amount_removed"] \
    - acct["bad_timestamp_removed"] - acct["orphans_set_aside"] == acct["clean_rows_remaining"], \
    "full accounting does not reconcile"
assert acct["orphans_set_aside"] + acct["clean_rows_remaining"] == acct["orders_after_ts_filter"], \
    "orphan + clean split should reconstruct the pre-split row count"
print("accounting check passed: orphans + clean == orders_after_ts_filter")


# part b: business questions
def write_csv(df, name):
    # coalesce(1) is fine, the results are tiny.
    df.coalesce(1).write.mode("overwrite").csv(f"{OUT_DIR}/{name}", header=True)


revenue = F.round(F.sum("amount"), 2).cast("decimal(18,2)").alias("revenue")  # decimal, no scientific notation
# only the columns we need, so the two country columns don't clash
cust = clean_customers.select("customer_id", "name", "signup_date")

# revenue and orders per country, api
b1_api = (
    clean_orders.groupBy("country")
    .agg(revenue, F.count("*").alias("orders"))
    .orderBy(F.desc("revenue"))
)
write_csv(b1_api, "b1_revenue_by_country")

# same thing in sql
clean_orders.createOrReplaceTempView("clean_orders")
b1_sql = spark.sql("""
    SELECT country, ROUND(SUM(amount), 2) AS revenue, COUNT(*) AS orders
    FROM clean_orders
    GROUP BY country
    ORDER BY revenue DESC
""")
write_csv(b1_sql, "b1_revenue_by_country_sql")
b1_api.explain()
b1_sql.explain()
# same plan both ways, sql text and api calls compile to one catalyst plan.

# top 10 customers
b2 = (
    clean_orders.join(cust, "customer_id")
    .groupBy("customer_id", "name")
    .agg(revenue)
    .orderBy(F.desc("revenue"))
    .limit(10)
)
write_csv(b2, "b2_top10_customers")

# revenue by category
b3 = (
    clean_orders.join(products, "product_id")
    .groupBy("category")
    .agg(revenue)
    .orderBy(F.desc("revenue"))
)
write_csv(b3, "b3_revenue_by_category")

# average order value by signup year
b4 = (
    clean_orders.join(cust, "customer_id")
    .groupBy(F.year(F.to_date("signup_date")).alias("signup_year"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_order_value"))
    .orderBy("signup_year")
)
write_csv(b4, "b4_aov_by_signup_year")

# monthly revenue for 2025
b5 = (
    clean_orders.filter(F.year("ts") == 2025)
    .groupBy(F.month("ts").alias("month"))
    .agg(revenue)
    .orderBy("month")
)
write_csv(b5, "b5_monthly_revenue_2025")

# best category per country
# window: rank categories inside each country, keep the top one.
cat_rev = (
    clean_orders.join(products, "product_id")
    .groupBy("country", "category")
    .agg(revenue)
)
w_best = Window.partitionBy("country").orderBy(F.desc("revenue"))
b6 = (
    cat_rev.withColumn("rn", F.row_number().over(w_best))
    .filter("rn = 1")
    .drop("rn")
    .orderBy("country")
)
write_csv(b6, "b6_best_category_per_country")

for name, df in [("B1", b1_api), ("B2", b2), ("B3", b3), ("B4", b4), ("B5", b5), ("B6", b6)]:
    print(f"--- {name} ---")
    df.show(10, truncate=False)


# part c: make it fast
# helpers
sc = spark.sparkContext
UI_API = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
print("Spark UI:", sc.uiWebUrl)


def ui_get(path):
    with urllib.request.urlopen(f"{UI_API}/{path}") as r:
        return json.load(r)


def consume(df):
    # noop just runs the plan, nothing is written
    df.write.format("noop").mode("overwrite").save()


def run(label, fn):
    # label the job so it's easy to find in the ui
    sc.setJobDescription(label)
    t0 = time.time()
    result = fn()
    secs = time.time() - t0
    time.sleep(1)  # give the ui a moment to catch up
    stages = [s for s in ui_get("stages") if s.get("description") == label]
    print(f"\n[{label}] wall time {secs:.2f}s")
    for s in stages:
        print(f"   stage {s['stageId']:>3} {s['status']:<9} tasks={s['numTasks']:>4} "
              f"shuffle_read={s['shuffleReadBytes']:>10,}B shuffle_write={s['shuffleWriteBytes']:>10,}B "
              f"run_time={s['executorRunTime']:>6}ms")
    sc.setJobDescription(None)
    return result, secs


def task_spread(label):
    # median vs max records in the last stage, shows the skew
    reducers = [s for s in ui_get("stages")
                if s.get("description") == label and s["shuffleReadRecords"] > 0]
    s = max(reducers, key=lambda st: st["stageId"])
    tasks = ui_get(f"stages/{s['stageId']}/{s['attemptId']}/taskList?length=10000")
    reads = [t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in tasks if t.get("taskMetrics")]
    print(f"   [{label}] stage {s['stageId']}: {len(reads)} tasks, "
          f"median records={statistics.median(reads):,.0f}, max records={max(reads):,}")


# the slow setup from the old job
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", "false")

cores = sc.defaultParallelism
# 2x cores keeps every core busy without lots of tiny tasks, 200 is way too many for 1m rows.
SHUFFLE_PARTS = cores * 2
print(f"cores={cores}, chosen shuffle partitions={SHUFFLE_PARTS}")

# rename country so the two country columns don't clash
cust_country = clean_customers.select("customer_id", F.col("country").alias("customer_country"))


def revenue_per_country(customers):
    return (
        clean_orders.join(customers, "customer_id")
        .groupBy("customer_country").agg(F.sum("amount").alias("revenue"))
    )


# broadcast join
spark.conf.set("spark.sql.shuffle.partitions", 200)
before = revenue_per_country(cust_country)
before.explain()  # sortmergejoin
run("C1 before: sort-merge join", lambda: consume(before))

after = revenue_per_country(F.broadcast(cust_country))
after.explain()  # broadcasthashjoin
run("C1 after: broadcast join", lambda: consume(after))

# shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 200)
run("C2 before: 200 partitions", lambda: consume(revenue_per_country(F.broadcast(cust_country))))
spark.conf.set("spark.sql.shuffle.partitions", SHUFFLE_PARTS)
run(f"C2 after: {SHUFFLE_PARTS} partitions", lambda: consume(revenue_per_country(F.broadcast(cust_country))))

# skew and aqe
spark.conf.set("spark.sql.shuffle.partitions", 200)
by_country = clean_orders.groupBy("country").agg(F.sum("amount").alias("revenue"))
run("C3 before: AQE off", lambda: consume(by_country))
task_spread("C3 before: AQE off")

spark.conf.set("spark.sql.adaptive.enabled", "true")
run("C3 after: AQE on", lambda: consume(by_country))
task_spread("C3 after: AQE on")
# aqe merged the mostly empty partitions into a few tasks.
# it can't fix skew in a groupBy (all the NP rows still land in one task), it only splits skewed joins.
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", SHUFFLE_PARTS)

# cache
joined = (
    clean_orders.filter(F.col("ts") >= "2025-01-01")
    .join(F.broadcast(products), "product_id")
)


def agg_category():
    return joined.groupBy("category").agg(F.sum("amount").alias("revenue"))


def agg_month():
    return joined.groupBy(F.month("ts").alias("month")).count()


agg_category().explain()  # starts from scan csv
run("C4 before: agg 1", lambda: consume(agg_category()))
run("C4 before: agg 2", lambda: consume(agg_month()))

joined.cache()
run("C4 materialize cache", lambda: joined.count())
agg_category().explain()  # inmemorytablescan at the bottom
run("C4 after: agg 1", lambda: consume(agg_category()))
run("C4 after: agg 2", lambda: consume(agg_month()))
print("Storage tab (cached partitions, memory B, disk B):",
      [(r["numCachedPartitions"], r["memoryUsed"], r["diskUsed"]) for r in ui_get("storage/rdd")])

joined.unpersist(blocking=True)
time.sleep(1)
print("Storage tab after unpersist:", ui_get("storage/rdd"))

# udf vs when/otherwise
@F.udf("string")
def size_label(a):
    return "large" if a > 200 else ("medium" if a > 50 else "small")


def udf_version():
    # collect is fine, only 3 rows
    return (clean_orders.withColumn("size", size_label("amount"))
            .groupBy("size").count().orderBy("size").collect())


def builtin_version():
    size = (F.when(F.col("amount") > 200, "large")
            .when(F.col("amount") > 50, "medium")
            .otherwise("small"))
    # collect is fine, only 3 rows
    return clean_orders.withColumn("size", size).groupBy("size").count().orderBy("size").collect()


run("C5 udf run 1", udf_version)
udf_rows, udf_secs = run("C5 udf run 2", udf_version)
run("C5 builtin run 1", builtin_version)
builtin_rows, builtin_secs = run("C5 builtin run 2", builtin_version)
assert udf_rows == builtin_rows, "UDF and when/otherwise counts differ"
print(f"C5 counts identical: {[tuple(r) for r in builtin_rows]}")
print(f"C5 second-run time: udf={udf_secs:.2f}s builtin={builtin_secs:.2f}s")


# keep the session alive for ui screenshots
if sys.stdin.isatty():
    input(f"\nSpark UI is live at {sc.uiWebUrl} -- take screenshots, then press Enter to exit.")
spark.stop()
