from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder.appName("CleanAcademicData").getOrCreate()

input_path = "s3://academic-bucket-2026/raw/anonimized_data_students.csv"
output_path = "s3://academic-bucket-2026/curated/academic_cleaned"

df = spark.read.option("header", True).csv(input_path)

df_clean = (
    df
    .withColumn("Promedio", regexp_replace(col("Promedio"), ",", ".").cast("double"))
    .withColumn("Inasistencias", col("Inasistencias").cast("int"))
    .withColumn("Creditos Curso", col("Creditos Curso").cast("int"))
    .dropDuplicates(["Codigo", "Cod_Curso", "Periodo"])
)

df_clean.write.mode("overwrite").parquet(output_path)
spark.stop()
