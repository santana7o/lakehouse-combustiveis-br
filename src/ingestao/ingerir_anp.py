# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão — CSVs mensais da ANP → Volume (landing)
# MAGIC
# MAGIC Baixa os arquivos novos de preços de gasolina/etanol para um Volume do
# MAGIC Unity Catalog. A camada Bronze (Auto Loader) pega daqui.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))  # torna `src/combustiveis` importável

from combustiveis.descoberta import baixar_novos

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "combustiveis")
dbutils.widgets.text("ano_inicial", "2025")

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
ano_inicial = int(dbutils.widgets.get("ano_inicial"))

destino = f"/Volumes/{catalogo}/{schema}/landing/anp"

# COMMAND ----------

novos = baixar_novos(destino, ano_inicial)
print(f"{len(novos)} arquivo(s) novo(s) em {destino}")
for nome in novos:
    print(" -", nome)
