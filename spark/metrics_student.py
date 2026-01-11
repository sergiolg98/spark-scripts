from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, when

spark = SparkSession.builder.appName("StudentMetrics").getOrCreate()

df = spark.read.parquet("s3://academic-bucket-2026/curated/academic_cleaned")

metrics = (
    df.groupBy("Codigo")
    .agg(
        avg("Promedio").alias("promedio_general"),
        sum("Inasistencias").alias("total_inasistencias"),
        sum(when(df.Promedio < 11, 1).otherwise(0)).alias("cursos_reprobados")
    )
    .withColumn(
        "riesgo_academico",
        when((col("promedio_general") < 11) | (col("total_inasistencias") > 30), "RIESGO")
        .otherwise("NORMAL")
    )
)

metrics.write.mode("overwrite").parquet(
    "s3://academic-data-2026/metrics/student_metrics"
)

spark.stop()
