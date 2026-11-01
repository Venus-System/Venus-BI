# Databricks notebook source
# MAGIC %md
# MAGIC # Venus - Catálogo, ingredientes e personalização (silver + gold)
# MAGIC
# MAGIC Complementa o pipeline de uso com o **catálogo** do Venus (ingredientes, composição
# MAGIC dos produtos, efeitos por perfil e score geral dos produtos) e cruza o score geral do
# MAGIC produto com o score personalizado que cada pessoa recebeu.
# MAGIC
# MAGIC | Tratamento | Por quê |
# MAGIC |---|---|
# MAGIC | Textos livres (descrição, resumo de segurança, fontes) ficam de fora | Não são usados nos gráficos e deixam as tabelas pesadas |
# MAGIC | Categoria vazia vira `Não classificado` | Evita buracos em gráficos e filtros |
# MAGIC | Risco de irritação 1-6 agrupado em faixas (baixo 1-2, médio 3-4, alto 5-6) | Facilita a leitura no dashboard |
# MAGIC | Comedogenicidade ≥ 3 = comedogênico | Corte usual da escala (0-5) |
# MAGIC | Score geral do produto = média das versões | Um produto pode ter mais de uma versão de rótulo |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_ingrediente AS
# MAGIC SELECT
# MAGIC     ingredient_key,
# MAGIC     inci_name,
# MAGIC     COALESCE(NULLIF(common_name, ''), inci_name)                 AS nome,
# MAGIC     COALESCE(NULLIF(ingredient_category_name, ''), 'Não classificado') AS categoria_ingrediente,
# MAGIC     irritation_risk_level,
# MAGIC     CASE WHEN irritation_risk_level >= 5 THEN '3. alto'
# MAGIC          WHEN irritation_risk_level >= 3 THEN '2. médio'
# MAGIC          ELSE '1. baixo' END                                      AS faixa_irritacao,
# MAGIC     comedogenicity_score,
# MAGIC     COALESCE(comedogenicity_score >= 3, false)                    AS is_comedogenico,
# MAGIC     environmental_risk_level,
# MAGIC     CASE WHEN environmental_risk_level >= 5 THEN '3. alto'
# MAGIC          WHEN environmental_risk_level >= 3 THEN '2. médio'
# MAGIC          ELSE '1. baixo' END                                      AS faixa_ambiental,
# MAGIC     biodegradability_level,
# MAGIC     scientific_confidence
# MAGIC FROM workspace.bronze.dim_ingredient
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY ingredient_key ORDER BY updated_date DESC) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Composição: uma linha por ingrediente em cada produto
# MAGIC CREATE OR REPLACE TABLE workspace.silver.produto_ingrediente AS
# MAGIC SELECT
# MAGIC     pi.product_key, p.product_name, p.brand_name, p.product_category_name,
# MAGIC     pi.ingredient_key, i.nome AS ingrediente, i.categoria_ingrediente,
# MAGIC     pi.ingredient_position, COALESCE(pi.is_first_ingredient, false) AS is_primeiro,
# MAGIC     i.irritation_risk_level, i.faixa_irritacao, i.is_comedogenico,
# MAGIC     i.environmental_risk_level, i.faixa_ambiental
# MAGIC FROM workspace.bronze.fact_product_ingredient pi
# MAGIC JOIN workspace.silver.dim_ingrediente i ON i.ingredient_key = pi.ingredient_key
# MAGIC LEFT JOIN workspace.silver.dim_produto p ON p.product_key = pi.product_key
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY pi.product_key, pi.ingredient_key ORDER BY pi.ingredient_position) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Efeito de cada ingrediente para cada tag de perfil (pele, cabelo, saúde...)
# MAGIC CREATE OR REPLACE TABLE workspace.silver.efeito_ingrediente_perfil AS
# MAGIC SELECT
# MAGIC     e.ingredient_key, e.profile_tag_key,
# MAGIC     t.profile_tag_name AS tag_perfil, t.profile_tag_category AS categoria_tag,
# MAGIC     e.effect_category AS tipo_efeito, e.effect_strength AS intensidade, e.evidence_level AS evidencia
# MAGIC FROM workspace.bronze.fact_ingredient_effect e
# MAGIC LEFT JOIN workspace.bronze.dim_profile_tag t ON t.profile_tag_key = e.profile_tag_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Score geral do produto no catálogo (independe de quem analisa)
# MAGIC CREATE OR REPLACE TABLE workspace.silver.score_produto_catalogo AS
# MAGIC SELECT
# MAGIC     s.product_key, p.product_name, p.brand_name, p.product_category_name,
# MAGIC     ROUND(AVG(s.overall_score), 1)       AS score_catalogo,
# MAGIC     ROUND(AVG(s.health_score), 1)        AS score_saude_catalogo,
# MAGIC     ROUND(AVG(s.environmental_score), 1) AS score_ambiental_catalogo
# MAGIC FROM workspace.bronze.fact_product_score s
# MAGIC LEFT JOIN workspace.silver.dim_produto p ON p.product_key = s.product_key
# MAGIC GROUP BY s.product_key, p.product_name, p.brand_name, p.product_category_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.catalogo_resumo AS
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM workspace.silver.dim_ingrediente)                                    AS ingredientes_catalogados,
# MAGIC     (SELECT COUNT(DISTINCT ingredient_key) FROM workspace.silver.produto_ingrediente)          AS ingredientes_em_produtos,
# MAGIC     (SELECT COUNT(DISTINCT product_key) FROM workspace.silver.produto_ingrediente)             AS produtos_com_composicao,
# MAGIC     (SELECT ROUND(COUNT(*) / COUNT(DISTINCT product_key), 1) FROM workspace.silver.produto_ingrediente) AS media_ingredientes_por_produto,
# MAGIC     (SELECT ROUND(AVG(CAST(faixa_irritacao = '3. alto' AS INT)) * 100, 1) FROM workspace.silver.dim_ingrediente) AS pct_irritacao_alta,
# MAGIC     (SELECT ROUND(AVG(CAST(is_comedogenico AS INT)) * 100, 1) FROM workspace.silver.dim_ingrediente) AS pct_comedogenicos,
# MAGIC     (SELECT ROUND(AVG(score_catalogo), 1) FROM workspace.silver.score_produto_catalogo)        AS score_medio_catalogo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ingredientes do catálogo por faixa de risco (barras)
# MAGIC CREATE OR REPLACE TABLE workspace.gold.ingredientes_por_risco AS
# MAGIC SELECT categoria_ingrediente, faixa_irritacao, faixa_ambiental, is_comedogenico,
# MAGIC        COUNT(*) AS ingredientes
# MAGIC FROM workspace.silver.dim_ingrediente
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ingredientes mais presentes nos produtos (ranking)
# MAGIC CREATE OR REPLACE TABLE workspace.gold.ingredientes_mais_usados AS
# MAGIC SELECT ingrediente, categoria_ingrediente, faixa_irritacao, is_comedogenico,
# MAGIC        COUNT(DISTINCT product_key) AS produtos,
# MAGIC        SUM(CAST(is_primeiro AS INT)) AS vezes_primeiro_ingrediente,
# MAGIC        DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT product_key) DESC) AS ranking
# MAGIC FROM workspace.silver.produto_ingrediente
# MAGIC GROUP BY ingrediente, categoria_ingrediente, faixa_irritacao, is_comedogenico;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Composição por produto (boxplot e dispersão)
# MAGIC CREATE OR REPLACE TABLE workspace.gold.composicao_produto AS
# MAGIC SELECT
# MAGIC     c.product_key, c.product_name, c.brand_name, c.product_category_name,
# MAGIC     COUNT(*)                                          AS ingredientes,
# MAGIC     SUM(CAST(c.faixa_irritacao = '3. alto' AS INT))   AS ingredientes_irritacao_alta,
# MAGIC     SUM(CAST(c.is_comedogenico AS INT))               AS ingredientes_comedogenicos,
# MAGIC     MAX(c.irritation_risk_level)                      AS pior_risco_irritacao,
# MAGIC     MAX(s.score_catalogo)                             AS score_catalogo
# MAGIC FROM workspace.silver.produto_ingrediente c
# MAGIC LEFT JOIN workspace.silver.score_produto_catalogo s ON s.product_key = c.product_key
# MAGIC GROUP BY c.product_key, c.product_name, c.brand_name, c.product_category_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quantos ingredientes beneficiam ou oferecem risco a cada perfil
# MAGIC CREATE OR REPLACE TABLE workspace.gold.efeitos_por_perfil AS
# MAGIC SELECT categoria_tag, tag_perfil, tipo_efeito,
# MAGIC        COUNT(DISTINCT ingredient_key) AS ingredientes
# MAGIC FROM workspace.silver.efeito_ingrediente_perfil
# MAGIC WHERE tag_perfil IS NOT NULL
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Score geral do produto x score que cada pessoa recebeu (efeito da personalização)
# MAGIC CREATE OR REPLACE TABLE workspace.gold.personalizacao_por_produto AS
# MAGIC SELECT
# MAGIC     sp.origem_dado, sp.product_key, sp.product_name, sp.brand_name, sp.product_category_name,
# MAGIC     COUNT(*)                                       AS classificacoes,
# MAGIC     MAX(c.score_catalogo)                          AS score_catalogo,
# MAGIC     ROUND(AVG(sp.final_score), 1)                  AS score_personalizado,
# MAGIC     ROUND(AVG(sp.final_score) - MAX(c.score_catalogo), 1) AS diferenca,
# MAGIC     SUM(CAST(sp.is_contraindicado AS INT))         AS contraindicacoes
# MAGIC FROM workspace.silver.fato_score_personalizado sp
# MAGIC LEFT JOIN workspace.silver.score_produto_catalogo c ON c.product_key = sp.product_key
# MAGIC GROUP BY sp.origem_dado, sp.product_key, sp.product_name, sp.brand_name, sp.product_category_name;

# COMMAND ----------

# Verificação: nenhum produto ou ingrediente pode se perder entre bronze e silver/gold
problemas = []
n_bronze = spark.sql("SELECT COUNT(DISTINCT ingredient_key) FROM workspace.bronze.dim_ingredient").first()[0]
n_silver = spark.table("workspace.silver.dim_ingrediente").count()
print(f"ingredientes: bronze={n_bronze} silver={n_silver}")
if n_bronze != n_silver: problemas.append("dim_ingrediente perdeu linhas")

n_sp = spark.table("workspace.silver.fato_score_personalizado").count()
n_g = spark.sql("SELECT COALESCE(SUM(classificacoes),0) FROM workspace.gold.personalizacao_por_produto").first()[0]
print(f"classificações: silver={n_sp} gold={n_g}")
if n_sp != n_g: problemas.append("personalizacao_por_produto não bate com a silver")

if problemas:
    raise Exception("Problemas no catálogo: " + "; ".join(problemas))
print("Catálogo concluído com sucesso.")
