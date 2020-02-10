# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://containername@storageaccountname.blob.core.windows.net",
  mount_point = "/mnt/containername",
  extra_configs = {"fs.azure.account.key.storageaccountname.blob.core.windows.net":dbutils.secrets.get(scope = "databricks_scope", key = "blob2vmblob")})

# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

sampledf=spark.read.json("/mnt/containername/small_radio_json.json")

# COMMAND ----------

display(sampledf)

# COMMAND ----------

newDF=sampledf.select("firstName","Gender")

# COMMAND ----------

display(newDF)

# COMMAND ----------

newDF.write.mode("overwrite").csv("/mnt/containername/newfile.csv")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://outputfolder@storageaccountname.blob.core.windows.net",
  mount_point = "/mnt/containername",
  extra_configs = {"fs.azure.account.key.storageaccountname.blob.core.windows.net":dbutils.secrets.get(scope = "databricks_scope", key = "blob2vmblob")})

# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

newDF.write.mode("overwrite").csv("/mnt/containername/newfile.csv")
