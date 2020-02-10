# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://vmrootmaster@vmdatastoragedev.blob.core.windows.net",
  mount_point = "/mnt/vmrootmaster",
  extra_configs = {"fs.azure.account.key.vmdatastoragedev.blob.core.windows.net":dbutils.secrets.get(scope = "databricks_scope", key = "blob2vmblob")})

# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

sampledf=spark.read.json("/mnt/vmrootmaster/small_radio_json.json")

# COMMAND ----------

display(sampledf)

# COMMAND ----------

newDF=sampledf.select("firstName","Gender")

# COMMAND ----------

display(newDF)

# COMMAND ----------

newDF.write.mode("overwrite").csv("/mnt/vmrootmaster/newfile.csv")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://outputfolder@vmdatastoragedev.blob.core.windows.net",
  mount_point = "/mnt/outputfolder",
  extra_configs = {"fs.azure.account.key.vmdatastoragedev.blob.core.windows.net":dbutils.secrets.get(scope = "databricks_scope", key = "blob2vmblob")})

# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

newDF.write.mode("overwrite").csv("/mnt/outputfolder/newfile.csv")
