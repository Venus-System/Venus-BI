# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Validação cruzada: Databricks × Postgres
# MAGIC
# MAGIC O schema `venus_bi` do Postgres tem views analíticas (`vw_*`) que calculam KPIs
# MAGIC direto no banco, com CTEs e window functions (entrega de Modelagem de Dados).
# MAGIC
# MAGIC O pipeline do Databricks calcula os mesmos indicadores de forma independente
# MAGIC (bronze → silver → gold). Este notebook compara os dois resultados:
# MAGIC **se dois cálculos feitos em lugares diferentes chegam ao mesmo número, os dois estão certos.**
# MAGIC
# MAGIC | Verificação | Postgres (view) | Databricks (pipeline) |
# MAGIC |---|---|---|
# MAGIC | Análises por dia | `vw_analysis_temporal_windows` | `gold.analise_detalhe` |
# MAGIC | Análises concluídas por dia | `vw_analysis_temporal_windows` | `gold.analise_detalhe` |
# MAGIC | Total acumulado (window function) | `vw_analysis_temporal_windows` | soma acumulada no Spark |
# MAGIC | Análises e score por produto | `vw_analysis_kpi` | `silver.fato_analise` |
# MAGIC
# MAGIC **Diferença esperada e documentada:** o pipeline exclui as análises com falha do score
# MAGIC médio (na silver, o score delas vira nulo), enquanto a view do Postgres conta essas
# MAGIC análises com score 0. Por isso o score é comparado aplicando a regra do Postgres, e a
# MAGIC diferença entre as duas regras aparece separada.
# MAGIC
# MAGIC O resultado fica salvo em `workspace.gold.validacao_kpis`, como evidência para o relatório.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

jdbc_url = dbutils.secrets.get("venus", "jdbc_url")  # banco Venus-qa no Aiven; ver README
props = {
    "user": dbutils.secrets.get("venus", "bi_user"),
    "password": dbutils.secrets.get("venus", "bi_password"),
    "driver": "org.postgresql.Driver",
}

TOLERANCIA = 0.01  # o Postgres arredonda médias para 2 casas

def ler_postgres(objeto):
    return spark.read.jdbc(jdbc_url, objeto, properties=props)

resultados = []  # (verificacao, status, detalhe)

def registrar(verificacao, status, detalhe):
    resultados.append((verificacao, status, detalhe))
    print(f"{status:12} {verificacao}: {detalhe}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inventário: o que existe no `venus_bi`

# COMMAND ----------

try:
    inventario = ler_postgres(
        "(SELECT table_name AS view, "
        "CASE WHEN table_name LIKE 'dim_%' THEN 'dimensão' "
        "     WHEN table_name LIKE 'fact_%' THEN 'fato' "
        "     ELSE 'view analítica' END AS tipo "
        "FROM information_schema.views WHERE table_schema = 'venus_bi') q"
    )
    display(inventario.orderBy("tipo", "view"))
except Exception as e:
    print("Não foi possível listar as views:", e)

# COMMAND ----------

# Técnicas de SQL avançado que o banco documenta (entrega de Modelagem)
try:
    display(ler_postgres("venus_bi.vw_sql_advanced_requirements_status"))
except Exception as e:
    print("View de requisitos indisponível:", str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Análises por dia: `vw_analysis_temporal_windows` × pipeline

# COMMAND ----------

try:
    pg = (
        ler_postgres("venus_bi.vw_analysis_temporal_windows")
        .select(
            "date_key",
            F.col("analysis_count").cast("long").alias("analises_pg"),
            F.col("completed_analysis_count").cast("long").alias("concluidas_pg"),
            F.col("running_analysis_count").cast("long").alias("acumulado_pg"),
            F.col("avg_overall_score").cast("double").alias("score_pg"),
        )
    )

    detalhe = spark.table("workspace.gold.analise_detalhe")
    dbx = (
        detalhe.groupBy("date_key")
        .agg(
            F.count("*").alias("analises_dbx"),
            F.sum(F.when(F.col("analysis_status") == "completed", 1).otherwise(0)).alias("concluidas_dbx"),
            # Regra do Postgres: falha conta como score 0
            F.round(F.avg(F.coalesce("overall_score", F.lit(0))), 2).alias("score_dbx_regra_pg"),
            # Regra do pipeline: falha fica fora da média
            F.round(F.avg("overall_score"), 2).alias("score_dbx_pipeline"),
        )
        .withColumn(
            "acumulado_dbx",
            F.sum("analises_dbx").over(
                Window.orderBy("date_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    comparacao_dia = (
        pg.join(dbx, "date_key", "full_outer")
        .withColumn("ok_analises", F.coalesce(F.col("analises_pg") == F.col("analises_dbx"), F.lit(False)))
        .withColumn("ok_concluidas", F.coalesce(F.col("concluidas_pg") == F.col("concluidas_dbx"), F.lit(False)))
        .withColumn("ok_acumulado", F.coalesce(F.col("acumulado_pg") == F.col("acumulado_dbx"), F.lit(False)))
        .withColumn(
            "ok_score",
            F.coalesce(F.abs(F.col("score_pg") - F.col("score_dbx_regra_pg")) <= TOLERANCIA, F.lit(False)),
        )
        .orderBy("date_key")
    )
    display(comparacao_dia)

    dias = comparacao_dia.count()
    for coluna, nome in [
        ("ok_analises", "Análises por dia"),
        ("ok_concluidas", "Análises concluídas por dia"),
        ("ok_acumulado", "Total acumulado (window function)"),
        ("ok_score", "Score médio por dia"),
    ]:
        divergentes = comparacao_dia.filter(~F.col(coluna)).count()
        registrar(
            nome,
            "OK" if divergentes == 0 else "DIVERGENTE",
            f"{dias - divergentes} de {dias} dias iguais",
        )

    # Mostra o efeito da regra de tratamento das falhas (não é erro, é decisão de negócio)
    dif_regra = comparacao_dia.filter(
        F.col("score_dbx_regra_pg") != F.col("score_dbx_pipeline")
    ).count()
    registrar(
        "Efeito de excluir falhas do score",
        "INFORMATIVO",
        f"{dif_regra} dias têm score diferente entre a regra do Postgres e a do pipeline",
    )
except Exception as e:
    registrar("Análises por dia", "ERRO", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análises por produto: `vw_analysis_kpi` × pipeline

# COMMAND ----------

try:
    pg_prod = (
        ler_postgres("venus_bi.vw_analysis_kpi")
        .groupBy("product_key")
        .agg(
            F.sum("analysis_count").cast("long").alias("analises_pg"),
            # A view agrupa por dia e produto; refaz a média ponderada pelo número de análises
            F.round(
                F.sum(F.col("avg_overall_score") * F.col("analysis_count")) / F.sum("analysis_count"), 2
            ).cast("double").alias("score_pg"),
        )
    )

    dbx_prod = (
        spark.table("workspace.silver.fato_analise")
        .groupBy("product_key", "product_name")
        .agg(
            F.count("*").alias("analises_dbx"),
            F.round(F.avg(F.coalesce("overall_score", F.lit(0))), 2).alias("score_dbx_regra_pg"),
        )
    )

    comparacao_prod = (
        pg_prod.join(dbx_prod, "product_key", "full_outer")
        .withColumn("ok_analises", F.coalesce(F.col("analises_pg") == F.col("analises_dbx"), F.lit(False)))
        .withColumn(
            "ok_score",
            # Tolerância um pouco maior: a média ponderada usa médias já arredondadas pelo Postgres
            F.coalesce(F.abs(F.col("score_pg") - F.col("score_dbx_regra_pg")) <= 0.05, F.lit(False)),
        )
        .orderBy("product_key")
    )
    display(comparacao_prod)

    produtos = comparacao_prod.count()
    for coluna, nome in [("ok_analises", "Análises por produto"), ("ok_score", "Score médio por produto")]:
        divergentes = comparacao_prod.filter(~F.col(coluna)).count()
        registrar(nome, "OK" if divergentes == 0 else "DIVERGENTE", f"{produtos - divergentes} de {produtos} produtos iguais")
except Exception as e:
    registrar("Análises por produto", "ERRO", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resultado

# COMMAND ----------

validacao = (
    spark.createDataFrame(resultados, "verificacao string, status string, detalhe string")
    .withColumn("executado_em", F.current_timestamp())
)

# Guarda o histórico de todas as execuções (evidência para o relatório)
validacao.write.mode("append").saveAsTable("workspace.gold.validacao_kpis")

display(validacao)

falhas = [r for r in resultados if r[1] in ("DIVERGENTE", "ERRO")]
if falhas:
    raise Exception(
        "Validação encontrou diferenças entre Postgres e Databricks: "
        + "; ".join(f"{v} ({s})" for v, s, _ in falhas)
    )

print("Validação concluída: os KPIs do pipeline batem com as views do Postgres.")

# COMMAND ----------

pg = spark.read.jdbc(jdbc_url, "venus_bi.fact_analysis_result", properties=props)
dbx = spark.table("workspace.bronze.fact_analysis_result")

print("Postgres agora:", pg.count(), "análises")
print("Bronze (cópia):", dbx.count(), "análises")
print("Última cópia da bronze:", dbx.agg({"ingerido_em": "max"}).first()[0])