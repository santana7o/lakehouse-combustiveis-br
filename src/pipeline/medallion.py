"""Pipeline declarativo (Lakeflow Declarative Pipelines / DLT): Bronze → Silver → Gold."""

import sys

import dlt
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("bundle.sourcePath"))  # noqa: F821 (spark é injetado pelo runtime)

from combustiveis import transformacoes as t  # noqa: E402

CAMINHO_LANDING = spark.conf.get("landing.path")  # noqa: F821


# ---------------------------------------------------------------- Bronze
@dlt.table(
    name="bronze_precos",
    comment="CSVs brutos da ANP, ingeridos incrementalmente com Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_precos():
    bruto = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("pathGlobFilter", "*.csv")  # ignora os .tmp de downloads em andamento
        .option("cloudFiles.inferColumnTypes", "false")  # tudo string no Bronze
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(CAMINHO_LANDING)
    )
    return t.normalizar_colunas(bruto).select(
        "*",
        F.col("_metadata.file_name").alias("_arquivo_origem"),
        F.current_timestamp().alias("_ingerido_em"),
    )


# ---------------------------------------------------------------- Silver
@dlt.table(
    name="silver_precos",
    comment="Preços tipados, padronizados e deduplicados. Linhas inválidas são descartadas e contabilizadas.",
    table_properties={"quality": "silver"},
    cluster_by=["uf", "produto"],
)
@dlt.expect_or_drop("data_valida", "data_coleta IS NOT NULL")
@dlt.expect_or_drop("preco_positivo", "valor_venda > 0")
@dlt.expect_or_drop("preco_plausivel", "valor_venda < 20")
@dlt.expect_or_drop("cnpj_14_digitos", "length(cnpj_revenda) = 14")
@dlt.expect("uf_valida", "length(uf) = 2")
def silver_precos():
    return t.limpar_precos(dlt.read("bronze_precos"))


# ---------------------------------------------------------------- Gold
@dlt.table(name="gold_preco_semanal_uf", comment="Preço médio/mediano/mín/máx por semana, UF e produto.")
def gold_preco_semanal_uf():
    return t.preco_semanal_por_uf(dlt.read("silver_precos"))


@dlt.table(
    name="gold_paridade_etanol", comment="Razão etanol/gasolina por UF e semana (≤ 0,70 → etanol compensa)."
)
def gold_paridade_etanol():
    return t.paridade_etanol_gasolina(dlt.read("gold_preco_semanal_uf"))


@dlt.table(name="gold_ranking_bandeiras", comment="Bandeiras mais baratas por UF e produto.")
def gold_ranking_bandeiras():
    return t.ranking_bandeiras(dlt.read("silver_precos"))
