# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Camada Silver (tratamento)
# MAGIC
# MAGIC Lê as tabelas cruas de `workspace.bronze` e grava em `workspace.silver`
# MAGIC versões limpas e prontas para análise.
# MAGIC
# MAGIC **Tratamentos aplicados (vão para o relatório gerencial):**
# MAGIC
# MAGIC | Tratamento | Por quê |
# MAGIC |---|---|
# MAGIC | Remoção de duplicatas pela chave | Garante uma linha por registro, mesmo se a origem repetir dados |
# MAGIC | Remoção de `firebase_uid`, `user_name` e textos livres de review | LGPD: o BI não precisa saber quem é a pessoa, só o perfil |
# MAGIC | Perfis vazios viram `nao_informado` | Evita "buracos" nos gráficos e filtros |
# MAGIC | Scores de análises com falha viram nulos | Uma análise que falhou não tem score; o zero puxaria a média para baixo |
# MAGIC | Tempo de processamento ≤ 0 vira nulo; acima de 30 s é marcado como atípico | Valores impossíveis saem; valores extremos ficam, mas sinalizados |
# MAGIC | Nota de review fora de 1 a 5 vira nula | Nota 0 não existe na escala do app |
# MAGIC | Coluna `origem_dado` (real, simulado, seed_tecnico) | Permite separar dados reais dos de teste no dashboard |
# MAGIC | Junção dos fatos com usuário, produto e data | Deixa cada linha autoexplicativa para o dashboard |
# MAGIC | Estado padronizado em sigla (`uf`) e nome (`estado`); coordenadas com 1 casa decimal | LGPD: o mapa precisa da região, não do endereço |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Usuários: só características do perfil, sem nome nem identificador do Firebase (LGPD)
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_usuario AS
# MAGIC SELECT
# MAGIC     user_key,
# MAGIC     user_status,
# MAGIC     COALESCE(skin_type, 'nao_informado')        AS skin_type,
# MAGIC     COALESCE(skin_phototype, 'nao_informado')   AS skin_phototype,
# MAGIC     COALESCE(skin_sensitivity, 'nao_informado') AS skin_sensitivity,
# MAGIC     COALESCE(hair_pattern, 'nao_informado')     AS hair_pattern,
# MAGIC     COALESCE(scalp_type, 'nao_informado')       AS scalp_type,
# MAGIC     COALESCE(age_range, 'nao_informado')        AS age_range,
# MAGIC     COALESCE(gender, 'nao_informado')           AS gender,
# MAGIC     COALESCE(acne_prone, false)                 AS acne_prone,
# MAGIC     COALESCE(is_pregnant, false)                AS is_pregnant,
# MAGIC     COALESCE(has_hyperpigmentation, false)      AS has_hyperpigmentation,
# MAGIC     COALESCE(has_melasma, false)                AS has_melasma,
# MAGIC     COALESCE(has_rosacea, false)                AS has_rosacea,
# MAGIC     COALESCE(has_eczema, false)                 AS has_eczema,
# MAGIC     COALESCE(prefer_cruelty_free, false)        AS prefer_cruelty_free,
# MAGIC     COALESCE(prefer_vegan, false)               AS prefer_vegan,
# MAGIC     COALESCE(prefer_fragrance_free, false)      AS prefer_fragrance_free,
# MAGIC     user_profile_id IS NOT NULL                 AS tem_perfil_preenchido,
# MAGIC     created_date                                AS data_cadastro,
# MAGIC     -- Localização (LGPD): só estado, cidade e coordenadas com 1 casa decimal (~10 km); nada de endereço
# MAGIC     CASE WHEN upper(trim(state)) IN ('AC', 'ACRE') THEN 'AC' WHEN upper(trim(state)) IN ('AL', 'ALAGOAS') THEN 'AL' WHEN upper(trim(state)) IN ('AP', 'AMAPÁ') THEN 'AP' WHEN upper(trim(state)) IN ('AM', 'AMAZONAS') THEN 'AM' WHEN upper(trim(state)) IN ('BA', 'BAHIA') THEN 'BA' WHEN upper(trim(state)) IN ('CE', 'CEARÁ') THEN 'CE' WHEN upper(trim(state)) IN ('DF', 'DISTRITO FEDERAL') THEN 'DF' WHEN upper(trim(state)) IN ('ES', 'ESPÍRITO SANTO') THEN 'ES' WHEN upper(trim(state)) IN ('GO', 'GOIÁS') THEN 'GO' WHEN upper(trim(state)) IN ('MA', 'MARANHÃO') THEN 'MA' WHEN upper(trim(state)) IN ('MT', 'MATO GROSSO') THEN 'MT' WHEN upper(trim(state)) IN ('MS', 'MATO GROSSO DO SUL') THEN 'MS' WHEN upper(trim(state)) IN ('MG', 'MINAS GERAIS') THEN 'MG' WHEN upper(trim(state)) IN ('PA', 'PARÁ') THEN 'PA' WHEN upper(trim(state)) IN ('PB', 'PARAÍBA') THEN 'PB' WHEN upper(trim(state)) IN ('PR', 'PARANÁ') THEN 'PR' WHEN upper(trim(state)) IN ('PE', 'PERNAMBUCO') THEN 'PE' WHEN upper(trim(state)) IN ('PI', 'PIAUÍ') THEN 'PI' WHEN upper(trim(state)) IN ('RJ', 'RIO DE JANEIRO') THEN 'RJ' WHEN upper(trim(state)) IN ('RN', 'RIO GRANDE DO NORTE') THEN 'RN' WHEN upper(trim(state)) IN ('RS', 'RIO GRANDE DO SUL') THEN 'RS' WHEN upper(trim(state)) IN ('RO', 'RONDÔNIA') THEN 'RO' WHEN upper(trim(state)) IN ('RR', 'RORAIMA') THEN 'RR' WHEN upper(trim(state)) IN ('SC', 'SANTA CATARINA') THEN 'SC' WHEN upper(trim(state)) IN ('SP', 'SÃO PAULO') THEN 'SP' WHEN upper(trim(state)) IN ('SE', 'SERGIPE') THEN 'SE' WHEN upper(trim(state)) IN ('TO', 'TOCANTINS') THEN 'TO' END AS uf,
# MAGIC     CASE WHEN upper(trim(state)) IN ('AC', 'ACRE') THEN 'Acre' WHEN upper(trim(state)) IN ('AL', 'ALAGOAS') THEN 'Alagoas' WHEN upper(trim(state)) IN ('AP', 'AMAPÁ') THEN 'Amapá' WHEN upper(trim(state)) IN ('AM', 'AMAZONAS') THEN 'Amazonas' WHEN upper(trim(state)) IN ('BA', 'BAHIA') THEN 'Bahia' WHEN upper(trim(state)) IN ('CE', 'CEARÁ') THEN 'Ceará' WHEN upper(trim(state)) IN ('DF', 'DISTRITO FEDERAL') THEN 'Distrito Federal' WHEN upper(trim(state)) IN ('ES', 'ESPÍRITO SANTO') THEN 'Espírito Santo' WHEN upper(trim(state)) IN ('GO', 'GOIÁS') THEN 'Goiás' WHEN upper(trim(state)) IN ('MA', 'MARANHÃO') THEN 'Maranhão' WHEN upper(trim(state)) IN ('MT', 'MATO GROSSO') THEN 'Mato Grosso' WHEN upper(trim(state)) IN ('MS', 'MATO GROSSO DO SUL') THEN 'Mato Grosso do Sul' WHEN upper(trim(state)) IN ('MG', 'MINAS GERAIS') THEN 'Minas Gerais' WHEN upper(trim(state)) IN ('PA', 'PARÁ') THEN 'Pará' WHEN upper(trim(state)) IN ('PB', 'PARAÍBA') THEN 'Paraíba' WHEN upper(trim(state)) IN ('PR', 'PARANÁ') THEN 'Paraná' WHEN upper(trim(state)) IN ('PE', 'PERNAMBUCO') THEN 'Pernambuco' WHEN upper(trim(state)) IN ('PI', 'PIAUÍ') THEN 'Piauí' WHEN upper(trim(state)) IN ('RJ', 'RIO DE JANEIRO') THEN 'Rio de Janeiro' WHEN upper(trim(state)) IN ('RN', 'RIO GRANDE DO NORTE') THEN 'Rio Grande do Norte' WHEN upper(trim(state)) IN ('RS', 'RIO GRANDE DO SUL') THEN 'Rio Grande do Sul' WHEN upper(trim(state)) IN ('RO', 'RONDÔNIA') THEN 'Rondônia' WHEN upper(trim(state)) IN ('RR', 'RORAIMA') THEN 'Roraima' WHEN upper(trim(state)) IN ('SC', 'SANTA CATARINA') THEN 'Santa Catarina' WHEN upper(trim(state)) IN ('SP', 'SÃO PAULO') THEN 'São Paulo' WHEN upper(trim(state)) IN ('SE', 'SERGIPE') THEN 'Sergipe' WHEN upper(trim(state)) IN ('TO', 'TOCANTINS') THEN 'Tocantins' END AS estado,
# MAGIC     NULLIF(initcap(trim(city)), '')             AS cidade,
# MAGIC     ROUND(latitude, 1)                          AS latitude,
# MAGIC     ROUND(longitude, 1)                         AS longitude
# MAGIC FROM workspace.bronze.dim_user
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY updated_date DESC) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Produtos já com marca e categoria
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_produto AS
# MAGIC SELECT
# MAGIC     p.product_key,
# MAGIC     p.product_name,
# MAGIC     p.is_active,
# MAGIC     p.brand_key,
# MAGIC     COALESCE(p.brand_name, 'Sem marca')                AS brand_name,
# MAGIC     COALESCE(p.brand_country, 'nao_informado')         AS brand_country,
# MAGIC     COALESCE(b.is_brazilian, false)                    AS marca_brasileira,
# MAGIC     COALESCE(b.has_cruelty_free_claim, false)          AS marca_cruelty_free,
# MAGIC     COALESCE(b.has_vegan_claim, false)                 AS marca_vegana,
# MAGIC     p.product_category_key,
# MAGIC     COALESCE(p.product_category_name, 'Sem categoria') AS product_category_name
# MAGIC FROM workspace.bronze.dim_product p
# MAGIC LEFT JOIN workspace.bronze.dim_brand b ON b.brand_key = p.brand_key
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY p.product_key ORDER BY p.updated_date DESC) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calendário (só tira a coluna técnica de ingestão)
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_data AS
# MAGIC SELECT * EXCEPT (ingerido_em)
# MAGIC FROM workspace.bronze.dim_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fatos (já juntados com as dimensões)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Análises de produtos: uma linha por análise
# MAGIC CREATE OR REPLACE TABLE workspace.silver.fato_analise AS
# MAGIC WITH base AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.bronze.fact_analysis_result
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY analysis_result_key ORDER BY ingerido_em DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     a.analysis_result_key,
# MAGIC     a.date_key,
# MAGIC     d.year_month,
# MAGIC     d.week_number,
# MAGIC     d.weekday_name,
# MAGIC     d.is_weekend,
# MAGIC     a.analysis_status,
# MAGIC     a.analysis_status = 'completed' AS is_sucesso,
# MAGIC     -- Análise com falha não tem score: nulo em vez de zero
# MAGIC     CASE WHEN a.analysis_status = 'failed' THEN NULL ELSE a.overall_score END       AS overall_score,
# MAGIC     CASE WHEN a.analysis_status = 'failed' THEN NULL ELSE a.health_score END        AS health_score,
# MAGIC     CASE WHEN a.analysis_status = 'failed' THEN NULL ELSE a.environmental_score END AS environmental_score,
# MAGIC     CASE WHEN a.analysis_status = 'failed' THEN NULL ELSE a.ethical_score END       AS ethical_score,
# MAGIC     CASE WHEN a.analysis_status = 'failed' THEN NULL ELSE a.performance_score END   AS performance_score,
# MAGIC     CASE WHEN a.analysis_status = 'failed' THEN NULL ELSE a.transparency_score END  AS transparency_score,
# MAGIC     a.confidence_score,
# MAGIC     -- Tempo impossível vira nulo; tempo muito alto fica, mas marcado
# MAGIC     CASE WHEN a.processing_time_ms <= 0 THEN NULL ELSE a.processing_time_ms END AS processing_time_ms,
# MAGIC     COALESCE(a.processing_time_ms > 30000, false) AS is_tempo_atipico,
# MAGIC     CASE
# MAGIC         WHEN a.summary LIKE 'SEED T_CNICO%' THEN 'seed_tecnico'
# MAGIC         WHEN a.summary LIKE 'SEED BI%'      THEN 'simulado'
# MAGIC         ELSE 'real'
# MAGIC     END AS origem_dado,
# MAGIC     -- Perfil de quem analisou
# MAGIC     a.user_key,
# MAGIC     u.skin_type, u.skin_sensitivity, u.hair_pattern, u.age_range, u.gender, u.is_pregnant, u.acne_prone,
# MAGIC     -- Produto analisado
# MAGIC     a.product_key,
# MAGIC     p.product_name, p.brand_name, p.marca_brasileira, p.product_category_name
# MAGIC FROM base a
# MAGIC LEFT JOIN workspace.silver.dim_usuario u ON u.user_key = a.user_key
# MAGIC LEFT JOIN workspace.silver.dim_produto p ON p.product_key = a.product_key
# MAGIC LEFT JOIN workspace.silver.dim_data    d ON d.date_key = a.date_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classificação personalizada: o produto X para a pessoa Y
# MAGIC CREATE OR REPLACE TABLE workspace.silver.fato_score_personalizado AS
# MAGIC WITH base AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.bronze.fact_personalized_score
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY personalized_score_key ORDER BY ingerido_em DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     s.personalized_score_key,
# MAGIC     s.analysis_result_key,
# MAGIC     s.date_key,
# MAGIC     d.year_month,
# MAGIC     s.final_score,
# MAGIC     CAST(s.compatibility_percentage AS DOUBLE) AS compatibility_percentage,
# MAGIC     s.risk_level,
# MAGIC     s.recommendation_level,
# MAGIC     -- Ordem para os gráficos mostrarem do melhor para o pior
# MAGIC     CASE s.recommendation_level
# MAGIC         WHEN 'ideal'           THEN 1
# MAGIC         WHEN 'recommended'     THEN 2
# MAGIC         WHEN 'acceptable'      THEN 3
# MAGIC         WHEN 'not_recommended' THEN 4
# MAGIC         WHEN 'contraindicated' THEN 5
# MAGIC     END AS recommendation_ordem,
# MAGIC     s.recommendation_level = 'contraindicated' AS is_contraindicado,
# MAGIC     CASE
# MAGIC         WHEN s.summary LIKE 'SEED T_CNICO%' THEN 'seed_tecnico'
# MAGIC         WHEN s.summary LIKE 'SEED BI%'      THEN 'simulado'
# MAGIC         ELSE 'real'
# MAGIC     END AS origem_dado,
# MAGIC     s.user_key,
# MAGIC     u.skin_type, u.skin_sensitivity, u.hair_pattern, u.age_range, u.gender, u.is_pregnant, u.acne_prone,
# MAGIC     s.product_key,
# MAGIC     p.product_name, p.brand_name, p.product_category_name
# MAGIC FROM base s
# MAGIC LEFT JOIN workspace.silver.dim_usuario u ON u.user_key = s.user_key
# MAGIC LEFT JOIN workspace.silver.dim_produto p ON p.product_key = s.product_key
# MAGIC LEFT JOIN workspace.silver.dim_data    d ON d.date_key = s.date_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Avaliações: só a nota; título e comentário são texto livre e podem ter dados pessoais (LGPD)
# MAGIC CREATE OR REPLACE TABLE workspace.silver.fato_review AS
# MAGIC WITH base AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.bronze.fact_review
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY review_key ORDER BY ingerido_em DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     r.review_key,
# MAGIC     r.date_key,
# MAGIC     -- Nota fora da escala de 1 a 5 vira nula
# MAGIC     CASE WHEN r.rating BETWEEN 1 AND 5 THEN CAST(r.rating AS DOUBLE) END AS rating,
# MAGIC     r.verified_use,
# MAGIC     CASE
# MAGIC         WHEN r.title LIKE 'SEED T_CNICO%' THEN 'seed_tecnico'
# MAGIC         WHEN r.title LIKE 'SEED BI%'      THEN 'simulado'
# MAGIC         ELSE 'real'
# MAGIC     END AS origem_dado,
# MAGIC     r.user_key,
# MAGIC     u.skin_type, u.hair_pattern, u.age_range,
# MAGIC     r.product_key,
# MAGIC     p.product_name, p.brand_name, p.product_category_name
# MAGIC FROM base r
# MAGIC LEFT JOIN workspace.silver.dim_usuario u ON u.user_key = r.user_key
# MAGIC LEFT JOIN workspace.silver.dim_produto p ON p.product_key = r.product_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação de qualidade
# MAGIC
# MAGIC Confere se o tratamento não perdeu registros e se todo fato encontrou
# MAGIC seu usuário e seu produto. Se algo estiver errado, o notebook falha e
# MAGIC o Job não segue para a camada gold.

# COMMAND ----------

problemas = []

# 1) Nenhum registro pode se perder entre bronze e silver
pares = [
    ("fact_analysis_result", "analysis_result_key", "fato_analise"),
    ("fact_personalized_score", "personalized_score_key", "fato_score_personalizado"),
    ("fact_review", "review_key", "fato_review"),
]
for bronze, chave, silver in pares:
    n_bronze = spark.sql(f"SELECT COUNT(DISTINCT {chave}) FROM workspace.bronze.{bronze}").first()[0]
    n_silver = spark.table(f"workspace.silver.{silver}").count()
    status = "OK" if n_bronze == n_silver else "ERRO"
    print(f"{status:5} {silver}: bronze={n_bronze} silver={n_silver}")
    if n_bronze != n_silver:
        problemas.append(f"{silver} perdeu registros")

# 2) Todo fato precisa encontrar o usuário e o produto (sem "órfãos")
for silver in ["fato_analise", "fato_score_personalizado", "fato_review"]:
    sem_usuario = spark.sql(f"SELECT COUNT(*) FROM workspace.silver.{silver} WHERE skin_type IS NULL").first()[0]
    sem_produto = spark.sql(f"SELECT COUNT(*) FROM workspace.silver.{silver} WHERE product_name IS NULL").first()[0]
    status = "OK" if sem_usuario == 0 and sem_produto == 0 else "AVISO"
    print(f"{status:5} {silver}: sem usuário={sem_usuario} sem produto={sem_produto}")

if problemas:
    raise Exception("Problemas na camada silver: " + "; ".join(problemas))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quantos registros de cada origem existem hoje
# MAGIC SELECT 'fato_analise' AS tabela, origem_dado, COUNT(*) AS linhas FROM workspace.silver.fato_analise GROUP BY origem_dado
# MAGIC UNION ALL
# MAGIC SELECT 'fato_score_personalizado', origem_dado, COUNT(*) FROM workspace.silver.fato_score_personalizado GROUP BY origem_dado
# MAGIC UNION ALL
# MAGIC SELECT 'fato_review', origem_dado, COUNT(*) FROM workspace.silver.fato_review GROUP BY origem_dado
# MAGIC ORDER BY tabela, origem_dado;