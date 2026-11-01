# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada Bronze (ingestão)
# MAGIC
# MAGIC Primeira etapa do pipeline do Venus. Copia os dados do banco da aplicação para o Databricks **sem alterar nada**,
# MAGIC só acrescentando a data e a hora da carga. As camadas seguintes (silver e gold) partem sempre desta cópia.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Origem | PostgreSQL no Aiven, banco `Venus-qa` (ambiente de testes da aplicação) |
# MAGIC | Schema lido | `venus_bi`: camada analítica em modelo estrela criada pela equipe de banco |
# MAGIC | Destino | `workspace.bronze`, uma tabela Delta por view |
# MAGIC | Acesso | Usuário `venus_bi`, só leitura; endereço e credenciais no cofre de segredos (escopo `venus`) |
# MAGIC | Frequência | Todo dia às 4h, como primeira tarefa do Job **Venus BI · pipeline diário** |
# MAGIC
# MAGIC **Por que uma camada bronze?** Se algum tratamento da silver ou da gold estiver errado, dá para refazer
# MAGIC a partir desta cópia sem consultar o banco da aplicação de novo. Ela também registra quando cada carga aconteceu.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Conexão com o banco da aplicação
# MAGIC
# MAGIC A leitura é feita via JDBC. O endereço do banco, o usuário e a senha **não ficam escritos no notebook**: são lidos do
# MAGIC cofre de segredos do Databricks (`dbutils.secrets`, escopo `venus`), que também esconde os valores se alguém tentar imprimi-los.
# MAGIC
# MAGIC Atenção: o PostgreSQL diferencia maiúsculas no nome do banco. O nome correto é `Venus-qa`, com **V** maiúsculo.
# MAGIC Para usar a produção quando o app for lançado, basta trocar o segredo `jdbc_url` para apontar para `Venus-2ano`.

# COMMAND ----------

jdbc_url = dbutils.secrets.get("venus", "jdbc_url")  # banco Venus-qa no Aiven; ver README

props = {
    "user": dbutils.secrets.get("venus", "bi_user"),
    "password": dbutils.secrets.get("venus", "bi_password"),
    "driver": "org.postgresql.Driver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schemas das camadas
# MAGIC
# MAGIC Cada camada da arquitetura medallion é um schema próprio no catálogo `workspace`.
# MAGIC O `IF NOT EXISTS` deixa a célula segura para rodar todos os dias: ela só cria o que ainda não existe.

# COMMAND ----------

for camada in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{camada}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingestão das 14 views
# MAGIC
# MAGIC Cada view do `venus_bi` vira uma tabela em `workspace.bronze` com o mesmo nome, mais a coluna `ingerido_em`
# MAGIC (data e hora da carga).
# MAGIC
# MAGIC | Grupo | Views | Uso |
# MAGIC |---|---|---|
# MAGIC | Uso do app | `fact_analysis_result`, `fact_personalized_score`, `fact_review` | Análises, scores personalizados e avaliações |
# MAGIC | Contexto | `dim_user`, `dim_product`, `dim_brand`, `dim_product_category`, `dim_date` | Perfil, produto, marca, categoria e calendário |
# MAGIC | Catálogo | `fact_product_score`, `dim_ingredient`, `dim_ingredient_category`, `fact_product_ingredient`, `fact_ingredient_effect`, `dim_profile_tag` | Composição e risco dos produtos (não depende de uso) |
# MAGIC
# MAGIC **Decisões:**
# MAGIC - **Carga total (`overwrite`)**: a cada execução a tabela é substituída pela versão atual do banco. Com o volume de hoje
# MAGIC   (centenas de linhas de uso), é mais simples e seguro do que carga incremental.
# MAGIC - **`overwriteSchema`**: se a equipe de banco acrescentar uma coluna na view, a tabela acompanha sem erro.
# MAGIC - **Uma falha não para o loop**: o erro de cada view é anotado e as demais continuam, para mostrar todos os problemas de uma vez.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

views = ["dim_date", "dim_user", "dim_product", "dim_brand",
         "dim_product_category", "fact_analysis_result",
         "fact_personalized_score", "fact_review",
         # Catálogo (não depende de uso do app)
         "fact_product_score", "dim_ingredient", "dim_ingredient_category",
         "fact_product_ingredient", "fact_ingredient_effect", "dim_profile_tag"]

falhas = []

for v in views:
    try:
        df = (spark.read.jdbc(jdbc_url, f"venus_bi.{v}", properties=props)
                   .withColumn("ingerido_em", current_timestamp()))
        (df.write.mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(f"workspace.bronze.{v}"))
        print(f"OK    {v}: {df.count()} linhas")
    except Exception as e:
        falhas.append(v)
        print(f"ERRO  {v}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Controle de qualidade
# MAGIC
# MAGIC Se **qualquer** view falhou, o notebook termina com erro. No Job, isso faz a tarefa ficar vermelha e impede que
# MAGIC silver, gold, catálogo e validação rodem com dados incompletos: as tabelas finais ficam como estavam na última
# MAGIC execução bem-sucedida, e o dashboard nunca mostra números pela metade.

# COMMAND ----------

if falhas:
    raise Exception(f"Falha na ingestão de: {', '.join(falhas)}")

print("Bronze concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência das colunas
# MAGIC
# MAGIC Lista as colunas das tabelas de uso que a silver vai tratar. Serve para perceber rápido quando a equipe de banco
# MAGIC renomeia uma coluna, como já aconteceu com `hair_type`, que passou a se chamar `hair_pattern`.

# COMMAND ----------

tabelas = ["dim_date", "dim_user", "dim_product", "dim_brand",
           "dim_product_category", "fact_analysis_result",
           "fact_personalized_score", "fact_review"]

for t in tabelas:
    colunas = spark.table(f"workspace.bronze.{t}").columns
    print(f"{t}: {', '.join(colunas)}\n")
