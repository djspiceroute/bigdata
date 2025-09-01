#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyhive import hive
import argparse

def normalize(df, email_col="email"):
    return (
        df.withColumn("id",  F.when(F.col("id").rlike(r"^\d+$"), F.col("id").cast("int")))
          .withColumn("age", F.when(F.col("age").rlike(r"^\d+$"), F.col("age").cast("int")))
          .withColumn(email_col, F.when(F.trim(F.col(email_col))=="", None).otherwise(F.col(email_col)))
    )

def dq_summary(df, email_col="email", pk=("id",)):
    is_bad_email = F.col(email_col).isNull() | (~F.col(email_col).rlike(r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"))
    dup = (df.groupBy(*pk).count().filter(F.col("count") > 1)) if pk else None
    dup_count = 0
    if dup is not None:
        dup_count = (dup.select(F.sum(F.col("count") - 1).alias("dup"))
                        .collect()[0]["dup"]) or 0

    metrics = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(is_bad_email, 1).otherwise(0)).alias("missing_or_invalid_email")
    ).withColumn("dup_ids", F.lit(int(dup_count)))
    return metrics, dup, is_bad_email

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=10000)
    p.add_argument("--user", default="hive")
    p.add_argument("--db", default="demo")
    p.add_argument("--table", default="customers")
    p.add_argument("--email-col", default="email")
    p.add_argument("--pk", default="id", help="comma-separated list, e.g. id or id,country")
    args = p.parse_args()

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("dq-hive")
             .getOrCreate())

    # Fetch rows via PyHive (avoids JDBC column-qualifier issues)
    conn = hive.Connection(host=args.host, port=args.port, username=args.user, database=args.db)
    cur = conn.cursor()
    cur.execute(f"SELECT id, {args.email_col}, age, country FROM {args.table}")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()

    df = spark.createDataFrame(rows, cols)
    df2 = normalize(df, email_col=args.email_col)

    print("=== Cleaned sample ===")
    df2.show(truncate=False)

    pk_cols = tuple(c.strip() for c in args.pk.split(",") if c.strip())
    metrics_df, dup_keys, is_bad_email = dq_summary(df2, email_col=args.email_col, pk=pk_cols)

    print("=== DQ summary ===")
    metrics_df.show(truncate=False)

    print("=== Bad email rows ===")
    df2.filter(is_bad_email).show(truncate=False)

    if pk_cols:
        print("=== Duplicate id rows ===")
        join_on = pk_cols if len(pk_cols) > 1 else pk_cols[0]
        sort_cols = list(pk_cols)
        (df2.join(dup_keys.select(*pk_cols), on=join_on, how="inner")
            .orderBy(*sort_cols)
            .show(truncate=False))

    spark.stop()

if __name__ == "__main__":
    main()