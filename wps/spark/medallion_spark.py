"""Medallion build on a real Spark cluster.

THE DESIGN CONSTRAINT THAT MATTERS: this module must not reimplement a single
mapping. If the Spark path had its own transformations, the mapping logic would
exist twice -- once in the config-driven engine and once in Spark SQL -- and the
thesis would fail twice over: mappings back in code, and now two copies of them
free to drift apart.

So the engine is called INSIDE the executors. `map_record` is the same function
the local path uses; it is shipped to the workers and applied per partition.
Spark supplies distribution and shuffle. The bindings supply every mapping.
The two concerns stay separated, which is the whole point.

Run it with wps/spark/submit.sh, not directly.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

AS_OF = date(2026, 8, 30)


def _silver_schema(contested: list[str], uncontested: list[str]) -> StructType:
    fields = [
        StructField("service_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("contract_id", StringType()),
        StructField("period_id", StringType()),
        StructField("settlement_currency", StringType()),
        StructField("pricing_tier", StringType()),
        StructField("precision_degraded", BooleanType()),
        StructField("precision_quantum_minor", LongType()),
    ]
    for m in contested + uncontested:
        fields.append(StructField(m, LongType()))
        fields.append(StructField(f"{m}__canonical", LongType()))
        fields.append(StructField(f"{m}__rule", StringType()))
    return StructType(fields)


def build(spark: SparkSession, lake: str) -> dict:
    """Bronze -> Silver -> Gold, distributed."""
    from wps.config import load_bundle
    from wps.medallion import metric_names, service_order
    from wps.parse import parse
    from wps.pipeline import SOURCE_PATHS

    bundle = load_bundle()
    contested, uncontested = metric_names(bundle)
    order = service_order(bundle)
    schema = _silver_schema(contested, uncontested)

    # ---- Bronze -------------------------------------------------------
    # Parsing a native dialect is inherently whole-file work (a COBOL record
    # stream, one XML document, a multi-row CSV header), so parsing happens on
    # the driver and the RECORDS are then distributed. Splitting a proprietary
    # format mid-file would corrupt it; pretending otherwise would be theatre.
    bronze_counts = {}
    record_rdds = {}
    sc = spark.sparkContext
    for svc in order:
        recs = list(parse(bundle.bindings[svc], SOURCE_PATHS[svc]))
        bronze_counts[svc] = len(recs)
        payload = [json.dumps(
            {k: (None if v is None else json.dumps(v) if isinstance(v, list) else str(v))
             for k, v in r.items()}, sort_keys=True) for r in recs]
        rdd = sc.parallelize(payload, numSlices=int(
            os.environ.get("WPS_SHUFFLE_PARTITIONS", "2")))
        record_rdds[svc] = rdd
        bdf = (rdd.map(lambda p, s=svc: (s, p)).toDF(["_service_id", "payload"])
               .withColumn("_ingested_at", F.current_timestamp())
               .withColumn("_binding_hash", F.lit(bundle.binding_hashes[svc])))
        (bdf.write.format("delta").mode("overwrite")
            .save(f"{lake}/bronze_spark/{svc}"))

    # ---- Silver -------------------------------------------------------
    # The engine runs IN the executors. One bundle load per partition, not per
    # row -- the config is immutable for the run, so loading it per row would
    # be pure overhead.
    silver_dfs = []
    for svc in order:
        def conform(part_iter, service_id=svc):
            from wps.config import load_bundle as _load
            from wps.engine import map_record
            from wps.io.decryption import default_provider
            b = _load()
            binding = b.bindings[service_id]
            provider = default_provider()
            declared = binding["source"].get("max_decimal_places")
            ctd, unc = metric_names(b)
            out = []
            for payload in part_iter:
                raw = json.loads(payload)
                rec = {k: (json.loads(v) if v and v.startswith("[") else v)
                       for k, v in raw.items()}
                m = map_record(rec, binding, b, provider)
                if m.errors:
                    continue
                v = m.values
                mid = v.get("merchant.merchant_id")
                cid = v.get("contract.contract_id")
                pid = v.get("period.period_id")
                if not (mid and cid and pid):
                    continue
                ccy = v.get("contract.settlement_currency")
                degraded = (declared is not None and ccy is not None
                            and b.minor_units(ccy) > declared)
                row = {
                    "service_id": service_id, "merchant_id": mid, "contract_id": cid,
                    "period_id": pid, "settlement_currency": ccy,
                    "pricing_tier": v.get("contract.pricing_tier"),
                    "precision_degraded": degraded,
                    "precision_quantum_minor": (
                        10 ** (b.minor_units(ccy) - declared) if degraded else 1),
                }
                for metric in ctd + unc:
                    p = f"quarterly_performance.{metric}"
                    val, canon = v.get(p), v.get(p + "__canonical")
                    row[metric] = int(val) if isinstance(val, (int, Decimal)) else None
                    row[f"{metric}__canonical"] = (
                        int(canon) if isinstance(canon, (int, Decimal)) else None)
                    row[f"{metric}__rule"] = m.rules.get(p)
                out.append(tuple(row.get(f.name) for f in schema.fields))
            return iter(out)

        df = spark.createDataFrame(record_rdds[svc].mapPartitions(conform), schema=schema)

        # Metric-narrow services emit one record per metric. Collapse onto the
        # canonical grain with a real shuffle -- this is the aggregation the
        # cluster is actually for.
        agg = [F.first(F.col(c), ignorenulls=True).alias(c)
               for c in df.columns
               if c not in ("service_id", "merchant_id", "contract_id", "period_id")]
        df = df.groupBy("service_id", "merchant_id", "contract_id", "period_id").agg(*agg)
        silver_dfs.append(df)

    silver = silver_dfs[0]
    for d in silver_dfs[1:]:
        silver = silver.unionByName(d)
    # Written first, then read back, rather than cached. Caching the union
    # keeps every partition resident in a driver that is already tight on a
    # shared 8GB Docker; re-reading the Delta table costs a scan and removes
    # the memory pressure entirely.
    (silver.write.format("delta").mode("overwrite")
        .save(f"{lake}/silver_spark"))
    silver = spark.read.format("delta").load(f"{lake}/silver_spark")
    silver_count = silver.count()

    # ---- Gold ---------------------------------------------------------
    # Reconciliation across services: a genuine wide shuffle on the contract
    # grain. Each service's own figure is preserved with the rule that made it.
    prec = {s: bundle.bindings[s].get("canonical_precedence", 999) for s in order}
    prec_expr = F.create_map([x for s, p in prec.items()
                              for x in (F.lit(s), F.lit(p))])
    silver = silver.withColumn("_prec", prec_expr[F.col("service_id")])

    grain = ["merchant_id", "contract_id", "period_id"]
    gold = silver.groupBy(*grain).agg(
        F.first(F.col("settlement_currency"), ignorenulls=True).alias("settlement_currency"),
        F.first(F.col("pricing_tier"), ignorenulls=True).alias("pricing_tier"),
        F.max(F.col("precision_degraded")).alias("precision_degraded"),
        F.sort_array(F.collect_set("service_id")).alias("_services"),
        *[F.first(F.col(m), ignorenulls=True).alias(m) for m in uncontested],
        # Rank ONLY the services that actually have a canonical value.
        #
        # min(struct(precedence, value)) over every row is the obvious
        # formulation and it is wrong: the highest-precedence service wins even
        # when it did not report the metric, and its null becomes Gold's
        # answer -- the exact "absent materialised as data" failure the
        # contract forbids. Nulling the precedence for non-reporting rows makes
        # min() skip them, because min() ignores nulls.
        *[F.min(F.when(F.col(f"{m}__canonical").isNotNull(),
                       F.struct(F.col("_prec").alias("p"),
                                F.col(f"{m}__canonical").alias("v"))))
           .alias(f"_canon_{m}") for m in contested],
        *[F.collect_list(F.struct(
            F.col("service_id").alias("s"), F.col(m).alias("v"),
            F.col(f"{m}__rule").alias("r"))).alias(f"_var_{m}") for m in contested],
    )

    for m in contested:
        # Lowest precedence number wins among the services that could actually
        # derive it; null when none could.
        gold = gold.withColumn(m, F.col(f"_canon_{m}.v"))
        gold = gold.withColumn(f"{m}_canonical_derivable", F.col(m).isNotNull())
        vals = F.expr(f"filter(transform(_var_{m}, x -> x.v), v -> v is not null)")
        gold = gold.withColumn(
            f"{m}_variance_pct",
            F.when(F.size(vals) > 1,
                   F.round(F.abs(F.array_max(vals) - F.array_min(vals))
                           / F.abs(F.coalesce(F.col(m), F.array_max(vals))) * 100, 2)))
        gold = gold.withColumn(f"{m}_by_source", F.to_json(F.map_from_entries(
            F.expr(f"transform(_var_{m}, x -> struct(x.s as key, "
                   f"struct(x.v as value, x.r as rule) as value))"))))
        gold = gold.drop(f"_canon_{m}", f"_var_{m}")

    gold = (gold
            .withColumn("source_services", F.concat_ws(",", F.col("_services")))
            .drop("_services")
            .withColumn("calendar_year", F.col("period_id").substr(1, 4).cast(IntegerType()))
            .withColumn("calendar_quarter",
                        F.substring_index(F.col("period_id"), "CQ", -1).cast(IntegerType()))
            .withColumn("contract_version", F.lit(bundle.contract_version))
            .withColumn("dictionary_version", F.lit(bundle.dictionary_version))
            .withColumn("bundle_hash", F.lit(bundle.bundle_hash))
            .withColumn("engine", F.lit("spark"))
            .withColumn("produced_at", F.current_timestamp()))

    st, cb = F.col("settled_txn_count"), F.col("chargeback_count")
    gold = gold.withColumn("dispute_ratio",
                           F.when((st > 0) & cb.isNotNull(), F.round(cb / st, 6)))

    (gold.write.format("delta").mode("overwrite").save(f"{lake}/gold_spark"))
    gold_count = gold.count()

    return {"bronze": bronze_counts, "silver": silver_count, "gold": gold_count,
            "executors": len([e for e in sc._jsc.sc().statusTracker()
                              .getExecutorInfos()]) - 1}


def main():
    lake = os.environ.get("WPS_LAKE", "/opt/wps/lake")
    spark = (SparkSession.builder
             .appName("wps-medallion")
             .config("spark.sql.extensions",
                     "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             # Shuffle partitions are sized from the cores actually granted,
             # not a fixed guess. On a one-core executor, 8 partitions means 8
             # sequential Python-worker round trips per stage -- the job does
             # not fail, it just crawls, which is harder to diagnose than a
             # crash. Read from the environment so the submit script owns it.
             .config("spark.sql.shuffle.partitions",
                     os.environ.get("WPS_SHUFFLE_PARTITIONS", "2"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    app_id = spark.sparkContext.applicationId
    master = spark.sparkContext.master
    print(f"\n{'=' * 66}\nWPS MEDALLION ON SPARK\n{'=' * 66}")
    print(f"application : {app_id}")
    print(f"master      : {master}")
    print(f"spark       : {spark.version}")

    result = build(spark, lake)

    print("-" * 66)
    print(f"BRONZE  {sum(result['bronze'].values()):6d} records  {result['bronze']}")
    print(f"SILVER  {result['silver']:6d} rows (conformed via the SAME config-driven engine)")
    print(f"GOLD    {result['gold']:6d} rows reconciled across services")
    print(f"executors used: {result['executors']}")
    print("=" * 66)
    spark.stop()


if __name__ == "__main__":
    main()
