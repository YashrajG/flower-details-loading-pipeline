# Databricks notebook source
storage_account = "incedoyashrajnonreldata"
container_name = "input-unstructured-data"
source_url = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net"

access_key = dbutils.secrets.get("sav2-acess-key", "data-pipeline-v2-acess-key")
mount_point_url = "/mnt/gen1dataset2"

extra_config_key = f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
extra_config_value = access_key

print(access_key)

# COMMAND ----------

dbutils.fs.mount(
    source = source_url,
    mount_point = mount_point_url,
    extra_configs = {
        extra_config_key: extra_config_value
    }
    )

# COMMAND ----------

dbutils.fs.ls(mount_point_url)

# COMMAND ----------


