# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Camada Gold (KPIs)
# MAGIC
# MAGIC Lê as tabelas tratadas de `workspace.silver` e grava em `workspace.gold`
# MAGIC as tabelas que alimentam o dashboard.
# MAGIC
# MAGIC Todas as tabelas guardam a coluna `origem_dado` (`real`, `simulado`, `seed_tecnico`).
# MAGIC No dashboard, o filtro padrão deve mostrar só `real` (e `simulado`, se a base de
# MAGIC demonstração estiver carregada). Assim, quando os dados reais chegarem, nada muda aqui.
# MAGIC
# MAGIC **Definição dos KPIs (vai para o relatório gerencial):**
# MAGIC
# MAGIC | KPI | Fórmula | Pergunta que responde |
# MAGIC |---|---|---|
# MAGIC | Total de análises | contagem de análises no período | O app está sendo usado? |
# MAGIC | Usuários ativos | usuários distintos que fizeram ao menos 1 análise | Quantas pessoas usam? |
# MAGIC | Taxa de sucesso | análises `completed` ÷ total × 100 | O motor de análise é confiável? |
# MAGIC | Score médio | média do `overall_score` (sem as análises com falha) | Os produtos analisados são bons? |
# MAGIC | Tempo médio de processamento | média do `processing_time_ms` | O app responde rápido? |
# MAGIC | % contraindicados | scores `contraindicated` ÷ total × 100 | Com que frequência o app alerta o usuário? |
# MAGIC | Score personalizado médio | média do final_score | Que nota o produto recebe para o perfil de quem analisa? |
# MAGIC | Cobertura da personalização | análises com score personalizado ÷ total × 100 | Em quantas análises o app conseguiu personalizar o resultado? |
# MAGIC | Desvio (variação) | (período atual − anterior) ÷ anterior × 100 | O indicador melhorou ou piorou? |
# MAGIC
# MAGIC Período atual = últimos 7 dias; período anterior = os 7 dias antes desses (a base de QA cobre poucas semanas).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cards de KPI com desvio

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.kpi_cards AS
# MAGIC WITH analises AS (
# MAGIC     SELECT
# MAGIC         origem_dado,
# MAGIC         CASE
# MAGIC             WHEN date_key >  date_sub(current_date(), 7) THEN 'atual'
# MAGIC             WHEN date_key >  date_sub(current_date(), 14) THEN 'anterior'
# MAGIC         END AS periodo,
# MAGIC         user_key, is_sucesso, overall_score, processing_time_ms
# MAGIC     FROM workspace.silver.fato_analise
# MAGIC ),
# MAGIC m_analise AS (
# MAGIC     SELECT
# MAGIC         origem_dado, periodo,
# MAGIC         CAST(COUNT(*) AS DOUBLE)                              AS total_analises,
# MAGIC         CAST(COUNT(DISTINCT user_key) AS DOUBLE)              AS usuarios_ativos,
# MAGIC         AVG(CAST(is_sucesso AS INT)) * 100                    AS taxa_sucesso,
# MAGIC         AVG(overall_score)                                    AS score_medio,
# MAGIC         AVG(processing_time_ms)                               AS tempo_medio_ms
# MAGIC     FROM analises
# MAGIC     WHERE periodo IS NOT NULL
# MAGIC     GROUP BY origem_dado, periodo
# MAGIC ),
# MAGIC scores AS (
# MAGIC     SELECT
# MAGIC         origem_dado,
# MAGIC         CASE
# MAGIC             WHEN date_key >  date_sub(current_date(), 7) THEN 'atual'
# MAGIC             WHEN date_key >  date_sub(current_date(), 14) THEN 'anterior'
# MAGIC         END AS periodo,
# MAGIC         is_contraindicado, compatibility_percentage, final_score
# MAGIC     FROM workspace.silver.fato_score_personalizado
# MAGIC ),
# MAGIC m_score AS (
# MAGIC     SELECT
# MAGIC         origem_dado, periodo,
# MAGIC         AVG(CAST(is_contraindicado AS INT)) * 100 AS pct_contraindicados,
# MAGIC         AVG(final_score)                         AS score_personalizado_medio,
# MAGIC         CAST(COUNT(*) AS DOUBLE)                 AS classificacoes
# MAGIC     FROM scores
# MAGIC     WHERE periodo IS NOT NULL
# MAGIC     GROUP BY origem_dado, periodo
# MAGIC ),
# MAGIC juntos AS (
# MAGIC     SELECT
# MAGIC         COALESCE(a.origem_dado, s.origem_dado) AS origem_dado,
# MAGIC         COALESCE(a.periodo, s.periodo)         AS periodo,
# MAGIC         a.total_analises, a.usuarios_ativos, a.taxa_sucesso, a.score_medio, a.tempo_medio_ms,
# MAGIC         s.pct_contraindicados, s.score_personalizado_medio,
# MAGIC         try_divide(s.classificacoes, a.total_analises) * 100 AS cobertura_personalizacao
# MAGIC     FROM m_analise a
# MAGIC     FULL OUTER JOIN m_score s
# MAGIC       ON a.origem_dado = s.origem_dado AND a.periodo = s.periodo
# MAGIC ),
# MAGIC -- Transforma as colunas em linhas: uma linha por KPI (fica fácil fazer um card por KPI)
# MAGIC longo AS (
# MAGIC     SELECT origem_dado, periodo, kpi, valor
# MAGIC     FROM juntos
# MAGIC     LATERAL VIEW stack(8,
# MAGIC         'total_analises',        total_analises,
# MAGIC         'usuarios_ativos',       usuarios_ativos,
# MAGIC         'taxa_sucesso',          taxa_sucesso,
# MAGIC         'score_medio',           score_medio,
# MAGIC         'tempo_medio_ms',        tempo_medio_ms,
# MAGIC         'pct_contraindicados',   pct_contraindicados,
# MAGIC         'score_personalizado_medio', score_personalizado_medio,
# MAGIC         'cobertura_personalizacao', cobertura_personalizacao
# MAGIC     ) AS kpi, valor
# MAGIC )
# MAGIC SELECT
# MAGIC     origem_dado,
# MAGIC     kpi,
# MAGIC     CASE kpi
# MAGIC         WHEN 'total_analises'        THEN 'Total de análises'
# MAGIC         WHEN 'usuarios_ativos'       THEN 'Usuários ativos'
# MAGIC         WHEN 'taxa_sucesso'          THEN 'Taxa de sucesso (%)'
# MAGIC         WHEN 'score_medio'           THEN 'Score médio'
# MAGIC         WHEN 'tempo_medio_ms'        THEN 'Tempo médio (ms)'
# MAGIC         WHEN 'pct_contraindicados'   THEN 'Contraindicados (%)'
# MAGIC         WHEN 'score_personalizado_medio' THEN 'Score personalizado médio'
# MAGIC         WHEN 'cobertura_personalizacao' THEN 'Cobertura da personalização (%)'
# MAGIC     END AS kpi_nome,
# MAGIC     ROUND(MAX(CASE WHEN periodo = 'atual'    THEN valor END), 2) AS valor_atual,
# MAGIC     ROUND(MAX(CASE WHEN periodo = 'anterior' THEN valor END), 2) AS valor_anterior,
# MAGIC     ROUND(
# MAGIC         try_divide(
# MAGIC             MAX(CASE WHEN periodo = 'atual'    THEN valor END)
# MAGIC           - MAX(CASE WHEN periodo = 'anterior' THEN valor END),
# MAGIC             MAX(CASE WHEN periodo = 'anterior' THEN valor END)
# MAGIC         ) * 100, 1) AS variacao_pct,
# MAGIC     current_timestamp() AS calculado_em
# MAGIC FROM longo
# MAGIC GROUP BY origem_dado, kpi;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Evolução diária (gráfico de linha)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.kpi_diario AS
# MAGIC WITH dia_analise AS (
# MAGIC     SELECT
# MAGIC         origem_dado, date_key,
# MAGIC         COUNT(*)                            AS analises,
# MAGIC         SUM(CAST(is_sucesso AS INT))        AS analises_sucesso,
# MAGIC         COUNT(DISTINCT user_key)            AS usuarios_ativos,
# MAGIC         AVG(overall_score)                  AS score_medio,
# MAGIC         AVG(processing_time_ms)             AS tempo_medio_ms
# MAGIC     FROM workspace.silver.fato_analise
# MAGIC     GROUP BY origem_dado, date_key
# MAGIC ),
# MAGIC dia_score AS (
# MAGIC     SELECT
# MAGIC         origem_dado, date_key,
# MAGIC         AVG(CAST(is_contraindicado AS INT)) * 100 AS pct_contraindicados
# MAGIC     FROM workspace.silver.fato_score_personalizado
# MAGIC     GROUP BY origem_dado, date_key
# MAGIC )
# MAGIC SELECT
# MAGIC     a.origem_dado,
# MAGIC     a.date_key,
# MAGIC     a.analises,
# MAGIC     a.analises_sucesso,
# MAGIC     a.usuarios_ativos,
# MAGIC     ROUND(try_divide(a.analises_sucesso, a.analises) * 100, 2) AS taxa_sucesso,
# MAGIC     ROUND(a.score_medio, 2)        AS score_medio,
# MAGIC     ROUND(a.tempo_medio_ms, 0)     AS tempo_medio_ms,
# MAGIC     ROUND(s.pct_contraindicados, 2) AS pct_contraindicados,
# MAGIC     -- Total acumulado de análises até o dia
# MAGIC     SUM(a.analises) OVER (
# MAGIC         PARTITION BY a.origem_dado ORDER BY a.date_key
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS analises_acumuladas,
# MAGIC     -- Média móvel de 7 dias do score (suaviza a linha)
# MAGIC     ROUND(AVG(a.score_medio) OVER (
# MAGIC         PARTITION BY a.origem_dado ORDER BY a.date_key
# MAGIC         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ), 2) AS score_media_movel_7d,
# MAGIC     -- Variação do score em relação ao dia anterior com dados
# MAGIC     ROUND(a.score_medio - LAG(a.score_medio) OVER (
# MAGIC         PARTITION BY a.origem_dado ORDER BY a.date_key
# MAGIC     ), 2) AS variacao_score_dia_anterior
# MAGIC FROM dia_analise a
# MAGIC LEFT JOIN dia_score s
# MAGIC   ON s.origem_dado = a.origem_dado AND s.date_key = a.date_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Perfil × classificação (pergunta central do dashboard)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quantas classificações de cada nível cada perfil recebeu (barras empilhadas)
# MAGIC CREATE OR REPLACE TABLE workspace.gold.recomendacao_por_perfil AS
# MAGIC SELECT
# MAGIC     origem_dado,
# MAGIC     skin_type,
# MAGIC     hair_pattern,
# MAGIC     age_range,
# MAGIC     skin_sensitivity,
# MAGIC     is_pregnant,
# MAGIC     recommendation_level,
# MAGIC     recommendation_ordem,
# MAGIC     COUNT(*)                                  AS classificacoes,
# MAGIC     ROUND(AVG(final_score), 2)                AS score_final_medio,
# MAGIC     ROUND(AVG(compatibility_percentage), 2)   AS compatibilidade_media
# MAGIC FROM workspace.silver.fato_score_personalizado
# MAGIC GROUP BY origem_dado, skin_type, hair_pattern, age_range, skin_sensitivity,
# MAGIC          is_pregnant, recommendation_level, recommendation_ordem;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Score por categoria e marca (gráfico de barras)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.score_por_produto AS
# MAGIC SELECT
# MAGIC     origem_dado,
# MAGIC     product_category_name,
# MAGIC     brand_name,
# MAGIC     marca_brasileira,
# MAGIC     product_name,
# MAGIC     COUNT(*)                               AS analises,
# MAGIC     ROUND(AVG(overall_score), 2)           AS score_medio,
# MAGIC     ROUND(AVG(health_score), 2)            AS score_saude,
# MAGIC     ROUND(AVG(environmental_score), 2)     AS score_ambiental,
# MAGIC     ROUND(AVG(ethical_score), 2)           AS score_etico,
# MAGIC     ROUND(AVG(CAST(is_sucesso AS INT)) * 100, 2) AS taxa_sucesso
# MAGIC FROM workspace.silver.fato_analise
# MAGIC GROUP BY origem_dado, product_category_name, brand_name, marca_brasileira, product_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Detalhe por análise (histograma e boxplot)
# MAGIC
# MAGIC Histograma e boxplot precisam dos valores individuais, não de médias.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.analise_detalhe AS
# MAGIC SELECT
# MAGIC     a.origem_dado,
# MAGIC     a.date_key,
# MAGIC     a.year_month,
# MAGIC     a.analysis_status,
# MAGIC     a.overall_score,
# MAGIC     a.processing_time_ms,
# MAGIC     a.is_tempo_atipico,
# MAGIC     a.skin_type,
# MAGIC     a.hair_pattern,
# MAGIC     a.age_range,
# MAGIC     a.product_category_name,
# MAGIC     a.brand_name,
# MAGIC     s.final_score,
# MAGIC     s.compatibility_percentage,
# MAGIC     s.recommendation_level
# MAGIC FROM workspace.silver.fato_analise a
# MAGIC LEFT JOIN workspace.silver.fato_score_personalizado s
# MAGIC   ON s.analysis_result_key = a.analysis_result_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Perfil da base de usuários
# MAGIC
# MAGIC Não depende de uso do app, então já mostra dados reais hoje.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.perfil_usuarios AS
# MAGIC SELECT
# MAGIC     skin_type,
# MAGIC     hair_pattern,
# MAGIC     age_range,
# MAGIC     skin_sensitivity,
# MAGIC     gender,
# MAGIC     acne_prone,
# MAGIC     is_pregnant,
# MAGIC     tem_perfil_preenchido,
# MAGIC     COUNT(*) AS usuarios
# MAGIC FROM workspace.silver.dim_usuario
# MAGIC WHERE user_status = 'active'
# MAGIC GROUP BY skin_type, hair_pattern, age_range, skin_sensitivity, gender,
# MAGIC          acne_prone, is_pregnant, tem_perfil_preenchido;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usuários por estado (mapa)
# MAGIC
# MAGIC Base do mapa do Brasil no dashboard. Conta contas ativas, contas com pelo menos uma análise e análises por estado.
# MAGIC Só entra quem tem estado preenchido no app. Nenhum dado individual sai daqui: cada linha é um estado inteiro.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.usuarios_por_estado AS
# MAGIC WITH u AS (
# MAGIC     SELECT user_key, uf, estado, latitude, longitude
# MAGIC     FROM workspace.silver.dim_usuario
# MAGIC     WHERE user_status = 'active' AND uf IS NOT NULL
# MAGIC ), a AS (
# MAGIC     SELECT user_key, COUNT(*) AS analises
# MAGIC     FROM workspace.silver.fato_analise
# MAGIC     WHERE origem_dado = 'real'
# MAGIC     GROUP BY user_key
# MAGIC )
# MAGIC SELECT
# MAGIC     u.uf,
# MAGIC     u.estado,
# MAGIC     COUNT(DISTINCT u.user_key)                    AS usuarios,
# MAGIC     COUNT(DISTINCT a.user_key)                    AS usuarios_com_analise,
# MAGIC     COALESCE(SUM(a.analises), 0)                  AS analises,
# MAGIC     ROUND(AVG(u.latitude), 1)                     AS latitude,
# MAGIC     ROUND(AVG(u.longitude), 1)                    AS longitude
# MAGIC FROM u LEFT JOIN a ON a.user_key = u.user_key
# MAGIC GROUP BY u.uf, u.estado;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação
# MAGIC
# MAGIC Confere se os KPIs batem com a camada silver. Se não baterem, o notebook falha
# MAGIC e o Job registra o erro.

# COMMAND ----------

problemas = []

# 1) A soma diária de análises tem que ser igual ao total da silver
total_silver = spark.table("workspace.silver.fato_analise").count()
total_gold = spark.sql("SELECT COALESCE(SUM(analises), 0) FROM workspace.gold.kpi_diario").first()[0]
print(f"{'OK' if total_silver == total_gold else 'ERRO':5} análises: silver={total_silver} gold(kpi_diario)={total_gold}")
if total_silver != total_gold:
    problemas.append("kpi_diario não bate com a silver")

# 2) Todo score personalizado tem que estar em recomendacao_por_perfil
total_scores = spark.table("workspace.silver.fato_score_personalizado").count()
total_rec = spark.sql("SELECT COALESCE(SUM(classificacoes), 0) FROM workspace.gold.recomendacao_por_perfil").first()[0]
print(f"{'OK' if total_scores == total_rec else 'ERRO':5} classificações: silver={total_scores} gold={total_rec}")
if total_scores != total_rec:
    problemas.append("recomendacao_por_perfil não bate com a silver")

# 3) A tabela de detalhe não pode duplicar análises
total_det = spark.table("workspace.gold.analise_detalhe").count()
print(f"{'OK' if total_det == total_silver else 'ERRO':5} detalhe: silver={total_silver} gold={total_det}")
if total_det != total_silver:
    problemas.append("analise_detalhe duplicou ou perdeu linhas")

if problemas:
    raise Exception("Problemas na camada gold: " + "; ".join(problemas))

print("\nGold concluída com sucesso.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Resumo das tabelas gold
# MAGIC SELECT 'kpi_cards' AS tabela, COUNT(*) AS linhas FROM workspace.gold.kpi_cards
# MAGIC UNION ALL SELECT 'kpi_diario', COUNT(*) FROM workspace.gold.kpi_diario
# MAGIC UNION ALL SELECT 'recomendacao_por_perfil', COUNT(*) FROM workspace.gold.recomendacao_por_perfil
# MAGIC UNION ALL SELECT 'score_por_produto', COUNT(*) FROM workspace.gold.score_por_produto
# MAGIC UNION ALL SELECT 'analise_detalhe', COUNT(*) FROM workspace.gold.analise_detalhe
# MAGIC UNION ALL SELECT 'perfil_usuarios', COUNT(*) FROM workspace.gold.perfil_usuarios;