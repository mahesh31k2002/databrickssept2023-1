# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure data lake using service principal
# MAGIC ### Space to fallow
# MAGIC #####1.Register Azure AD Application / Service Principal
# MAGIC #####2.Generate a secret /password for application
# MAGIC #####3.Set spark config with app /client id,Directory/ tenent id & secret
# MAGIC #####4.Assign Role 'Storage Blob data contributer' to the data lake

# COMMAND ----------

client_id="dafe9742-0dc0-4699-abd6-aab2092845a2"
tenent_id="c28ebd59-a842-4e64-b29c-e1ca7c272397"
client_secret="5_r8Q~sV04oGYoOuVFuG8BmBmdYwJR7asXg_ybm6"



# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sazelartraining.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sazelartraining.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sazelartraining.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sazelartraining.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sazelartraining.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sazelartraining.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.options(header='true').csv("abfss://demo@sazelartraining.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


