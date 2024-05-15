# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "78a39c11-3624-40a6-89b1-8a64c49713d6",
"fs.azure.account.oauth2.client.secret": 'QDw8Q~s2z0237HoqU8w.t5fK-cyg-.YSbyzlmbTe',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f88ea829-ec13-45df-92c3-822af8e99a1f/oauth2/token"}

dbutils.fs.mount(
source = "abfss://olympicstoragedatalake@tokyostoragedata.dfs.core.windows.net",
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolympic"

# COMMAND ----------

spark

# COMMAND ----------

atheletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/athelete.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
gender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/teams.csv")


# COMMAND ----------

atheletes.show()
coaches.show()
gender.show()
medals.show()
teams.show()

# COMMAND ----------

atheletes.printSchema()
coaches.printSchema()
gender.printSchema()
medals.printSchema()
teams.printSchema()

# COMMAND ----------

gender = gender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

medals = medals.withColumn("Rank", col("Rank").cast(IntegerType()))\
.withColumn("Gold", col("Gold").cast(IntegerType()))\
    .withColumn("Silver", col("Silver").cast(IntegerType()))\
        .withColumn("Bronze", col("Bronze").cast(IntegerType()))\
            .withColumn("Total", col("Total").cast(IntegerType()))\
                .withColumn("Rank by Total", col("Rank by Total").cast(IntegerType()))
            

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

top_gold_medal = medals.orderBy("Gold", ascending=False).show()

# COMMAND ----------

atheletes = atheletes.write.option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/atheletes")
coaches = coaches.write.option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/coaches")
gender = gender.write.option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/entriesgender")
medals = medals.write.option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/medals")
teams = teams.write.option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/teams")

# COMMAND ----------


