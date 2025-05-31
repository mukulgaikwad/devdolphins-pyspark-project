from pyspark.sql.functions import col, count, avg, percentile_approx, count_distinct, lit
from pyspark.sql.window import Window

def detect_patterns(spark, chunk_df, customer_importance_df, postgres_table):
    # Write chunk to PostgreSQL for incremental processing
    chunk_df.write.jdbc(
        url="jdbc:postgresql://<host>:<port>/transaction_pipeline",
        table=postgres_table,
        mode="append",
        properties={"user": "postgres", "password": "2707", "driver": "org.postgresql.Driver"}
    )

    # Read all transactions from PostgreSQL for pattern detection
    all_trans_df = spark.read.jdbc(
        url="jdbc:postgresql://<host>:<port>/transaction_pipeline",
        table=postgres_table,
        properties={"user": "postgres", "password": "2707", "driver": "org.postgresql.Driver"}
    )

    # PatId1: Top 1% by transaction count, bottom 20% by weight
    trans_count = all_trans_df.groupBy("MerchantId", "CustomerId").agg(count("*").alias("trans_count"))
    merchant_trans = all_trans_df.groupBy("MerchantId").agg(count("*").alias("total_trans"))
    valid_merchants = merchant_trans.filter(col("total_trans") > 50000)
    weighted_trans = trans_count.join(customer_importance_df, ["CustomerId"], "inner")
    weight_percentiles = weighted_trans.groupBy("MerchantId").agg(
        percentile_approx("weightage", 0.20).alias("weight_20th"),
        percentile_approx("trans_count", 0.99).alias("count_99th")
    )
    pat1_df = weighted_trans.join(valid_merchants, "MerchantId").join(weight_percentiles, "MerchantId").filter(
        (col("trans_count") >= col("count_99th")) & (col("weightage") <= col("weight_20th"))
    ).select(
        col("YStartTime"), col("detectionTime"), lit("PatId1").alias("patternId"),
        lit("UPGRADE").alias("ActionType"), col("CustomerId").alias("customerName"), col("MerchantId")
    )

    # PatId2: Low average transaction value
    trans_avg = all_trans_df.groupBy("MerchantId", "CustomerId").agg(
        count("*").alias("trans_count"),
        avg("transaction_value").alias("avg_value")
    )
    pat2_df = trans_avg.filter((col("trans_count") >= 80) & (col("avg_value") < 20)).select(
        col("YStartTime"), col("detectionTime"), lit("PatId2").alias("patternId"),
        lit("CHILD").alias("ActionType"), col("CustomerId").alias("customerName"), col("MerchantId")
    )

    # PatId3: Merchants with low female representation
    gender_counts = all_trans_df.groupBy("MerchantId", "gender").agg(count_distinct("CustomerId").alias("customer_count"))
    gender_pivot = gender_counts.groupBy("MerchantId").pivot("gender").sum("customer_count").fillna(0)
    pat3_df = gender_pivot.filter(
        (col("Female") >= 1000) & (col("Female") < 0.1 * col("Male"))
    ).select(
        col("YStartTime"), col("detectionTime"), lit("PatId3").alias("patternId"),
        lit("DEI-NEEDED").alias("ActionType"), col("MerchantId")
    )

    # Combine detections
    detections_df = pat1_df.union(pat2_df).union(pat3_df)
    return detections_df