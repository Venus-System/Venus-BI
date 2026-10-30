# Venus BI

Camada de Business Intelligence do **Venus**, recomendador personalizado de cosméticos (Projeto Interdisciplinar, 2º ano).

Este repositório guarda tudo o que roda no Databricks para transformar os dados do app em indicadores: os notebooks do pipeline, a configuração do Job que executa o pipeline todo dia, o dashboard e o relatório gerencial.

---

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Estrutura do repositório](#2-estrutura-do-repositório)
3. [De onde vêm os dados](#3-de-onde-vêm-os-dados)
4. [Como o pipeline funciona](#4-como-o-pipeline-funciona)
5. [O Job](#5-o-job)
6. [KPIs](#6-kpis)
7. [O dashboard](#7-o-dashboard)
8. [Como montar do zero](#8-como-montar-do-zero)
9. [Operação do dia a dia](#9-operação-do-dia-a-dia)
10. [Problemas conhecidos e soluções](#10-problemas-conhecidos-e-soluções)
11. [Limitações](#11-limitações)

---

## 1. Visão geral

O app do Venus grava cada análise de produto no PostgreSQL. A equipe de banco expõe esses dados num schema analítico, o `venus_bi`. O Databricks lê esse schema todo dia, trata os dados em três camadas e grava a base final que o dashboard consome.

```
 App Venus ──► PostgreSQL (Aiven)
                 banco Venus-qa
                 schema venus_bi (14 views, modelo estrela)
                      │
                      │ JDBC, usuário só leitura
                      ▼
 ┌─────────────────────── Databricks · Job "Venus BI · pipeline diário" (04:00) ───────────────────────┐
 │                                                                                                      │
 │   01 Bronze ──► 02 Silver ──┬──► 03 Gold ──────┬──► 05 Validação ──► compara com o Postgres         │
 │   (cópia fiel)  (tratamento) └──► 04 Catálogo ──┘                                                    │
 │                                                                                                      │
 └──────────────────────────────────────────────┬───────────────────────────────────────────────────────┘
                                                │ tabelas de workspace.gold
                                                ▼
                                      Dashboard AI/BI (5 páginas)
```

A arquitetura segue o padrão **medallion**: cada camada é um schema próprio no catálogo `workspace` do Unity Catalog.

| Camada | Schema | Papel | Tabelas |
|---|---|---|---|
| Bronze | `workspace.bronze` | Cópia fiel do banco, com data e hora da carga | 14 |
| Silver | `workspace.silver` | Dados limpos, sem dados pessoais e já unidos ao contexto | 10 |
| Gold | `workspace.gold` | Indicadores prontos para o dashboard | 14 |

---

## 2. Estrutura do repositório

```
notebooks/
  01_bronze.py        ingestão via JDBC                    (no workspace: Venus_Bronze)
  02_silver.py        tratamento e LGPD                    (Venus_Silver)
  03_gold.py          KPIs de uso e personalização         (Venus_Gold)
  04_catalogo.py      catálogo e efeito da personalização  (Venus_Catalogo)
  05_validacao.py     validação cruzada com o Postgres     (Venus_Validacao)
job/
  venus_bi_pipeline.json         configuração do Job
dashboard/
  venus_bi.lvdash.json           definição do dashboard AI/BI
docs/
  relatorio_gerencial_bi.html    relatório gerencial (abrir no navegador)
  Relatorio_Gerencial_BI_Venus.pdf   o mesmo relatório em PDF (A4)
pyproject.toml                   configuração do ruff (lint do CI)
```

Os notebooks estão no formato *source* do Databricks: arquivos `.py` em que `# COMMAND ----------` separa as células e `# MAGIC` marca as células de markdown e SQL. Eles abrem no Databricks como notebooks normais.

---

## 3. De onde vêm os dados

### Banco de origem

| Item | Valor |
|---|---|
| Servidor | PostgreSQL no Aiven |
| Banco | `Venus-qa` (ambiente de testes). A produção é `Venus-2ano` |
| Schema | `venus_bi`, criado pela equipe de banco na disciplina de Modelagem de Dados |
| Acesso | Usuário `venus_bi`, somente leitura nesse schema |

O banco de produção ainda não tem uso, porque o app não foi lançado. Por isso o pipeline lê o QA, que registra o uso real feito pela equipe durante os testes.

### Views lidas

| Grupo | Views | Para quê |
|---|---|---|
| Uso do app | `fact_analysis_result`, `fact_personalized_score`, `fact_review` | Cada análise, a nota personalizada para o perfil e as avaliações |
| Contexto | `dim_user`, `dim_product`, `dim_brand`, `dim_product_category`, `dim_date` | Perfil de quem analisou, produto, marca, categoria e calendário |
| Catálogo | `fact_product_score`, `dim_ingredient`, `dim_ingredient_category`, `fact_product_ingredient`, `fact_ingredient_effect`, `dim_profile_tag` | Composição dos produtos, risco dos ingredientes e efeito para cada perfil |

O `venus_bi` também tem views de KPI (`vw_analysis_temporal_windows`, `vw_analysis_kpi`). O pipeline não as usa para calcular nada: elas servem de gabarito na validação cruzada (seção 4.5).

---

## 4. Como o pipeline funciona

### 4.1 Bronze: ingestão (`01_bronze.py`)

1. Lê do cofre de segredos o endereço do banco (`jdbc_url`), o usuário e a senha.
2. Cria os schemas `bronze`, `silver` e `gold`, se ainda não existirem.
3. Para cada uma das 14 views, lê via JDBC, acrescenta a coluna `ingerido_em` e grava em `workspace.bronze.<view>`, substituindo a versão anterior (`overwrite` com `overwriteSchema`).
4. Se alguma view falhar, as demais continuam e, no fim, o notebook para com erro listando todas as que falharam.

Por que carga total e não incremental: o volume ainda é de centenas de linhas por tabela, e substituir tudo é mais simples e garante que a cópia sempre bate com o banco.

### 4.2 Silver: tratamento (`02_silver.py`)

| Tratamento | Motivo |
|---|---|
| Remoção de duplicatas pela chave, mantendo o registro mais recente | Uma linha por análise, score ou review |
| Exclusão de `firebase_uid`, nome do usuário e textos livres das reviews | LGPD: o BI precisa do perfil, não da identidade |
| Campos de perfil vazios viram `nao_informado` | Evita buracos em gráficos e filtros |
| Score de análise com falha vira nulo | Uma análise que falhou não deu nota. No Venus, 0 é um resultado válido (contraindicado) e não pode ser confundido com falha |
| Tempo de processamento ≤ 0 vira nulo; acima de 30 s é marcado como atípico | Valores impossíveis saem, valores extremos ficam sinalizados |
| Nota de review fora de 1 a 5 vira nula | A escala do app é de 1 a 5 |
| Coluna `origem_dado` (`real`, `simulado`, `seed_tecnico`) | Separa uso real de dados de carga técnica. O dashboard mostra `real` por padrão |
| Junção de cada fato com usuário, produto e data | Cada linha já carrega o contexto que o dashboard filtra |

Tabelas geradas: `dim_usuario`, `dim_produto`, `dim_data`, `fato_analise`, `fato_score_personalizado`, `fato_review`.

No fim, o notebook confere que nenhuma linha se perdeu entre bronze e silver e que todo fato encontrou seu usuário e seu produto. Se algo falhar, ele para com erro.

### 4.3 Gold: KPIs (`03_gold.py`)

| Tabela | Conteúdo | Usada em |
|---|---|---|
| `kpi_cards` | 8 KPIs com valor dos últimos 7 dias, dos 7 anteriores e desvio | Cards da Visão geral |
| `kpi_diario` | Análises, usuários, score e % contraindicados por dia, com acumulado e média móvel | Gráficos de linha |
| `recomendacao_por_perfil` | Classificações por nível de recomendação e característica de perfil | Recomendação por tipo de pele |
| `score_por_produto` | Análises e scores médios por produto | Ranking de produtos |
| `analise_detalhe` | Uma linha por análise, com perfil, produto e scores | Histograma, boxplot e filtros |
| `perfil_usuarios` | Contagem de usuários por combinação de perfil | Base de usuários |
| `usuarios_por_estado` | Contas ativas, contas com análise e análises por estado | Mapa do Brasil |

O notebook termina conferindo que a soma diária bate com o total da silver e que a tabela de detalhe não duplica análises.

### 4.4 Catálogo (`04_catalogo.py`)

Roda em paralelo com a gold, porque também só depende da silver.

- Silver do catálogo: `dim_ingrediente`, `produto_ingrediente`, `efeito_ingrediente_perfil`, `score_produto_catalogo`.
- Tratamentos: risco de irritação (escala de 1 a 6) agrupado em baixo, médio e alto; comedogenicidade ≥ 3 marcada como comedogênica; nota geral do produto = média das versões do rótulo.
- Gold: `catalogo_resumo`, `ingredientes_por_risco`, `ingredientes_mais_usados`, `composicao_produto`, `efeitos_por_perfil` e `personalizacao_por_produto`. Esta última compara a nota geral do catálogo com a nota personalizada de cada produto e responde à pergunta central do projeto.

### 4.5 Validação cruzada (`05_validacao.py`)

Volta ao Postgres e compara os números do pipeline com as views de KPI da equipe de banco, que calculam os mesmos indicadores por outro caminho (CTEs e funções de janela).

| Verificação | Gabarito no Postgres |
|---|---|
| Análises por dia | `vw_analysis_temporal_windows` |
| Análises concluídas por dia | `vw_analysis_temporal_windows` |
| Total acumulado | `vw_analysis_temporal_windows` |
| Score médio por dia | `vw_analysis_temporal_windows` |
| Análises por produto | `vw_analysis_kpi` |
| Score médio por produto | `vw_analysis_kpi` |
| Efeito de excluir falhas do score | informativo |

O resultado de cada execução é acrescentado a `workspace.gold.validacao_kpis`, que alimenta a página "Saúde do pipeline". Se alguma verificação der `DIVERGENTE` ou `ERRO`, o notebook para com erro e o Job fica vermelho.

---

## 5. O Job

| Item | Valor |
|---|---|
| Nome | Venus BI · pipeline diário |
| Agendamento | Todo dia às 04:00 (America/Sao_Paulo) |
| Computação | Serverless |
| Concorrência | Uma execução por vez, com fila |
| Arquivo | `job/venus_bi_pipeline.json` |

```
venusbronze ──► venussilver ──┬──► venusgold ──────┬──► venusvalidacao
                              └──► venuscatalogo ──┘
```

Cada tarefa tem a condição `ALL_SUCCESS`: só começa se todas as anteriores terminaram bem. Se uma etapa falha, as seguintes não rodam e as tabelas da gold ficam como estavam na última execução boa. Assim, o dashboard nunca mostra dados pela metade.

---

## 6. KPIs

Os cards comparam os **últimos 7 dias** com os **7 dias anteriores**, contados a partir da data da execução. O desvio é a variação percentual entre as duas janelas.

| KPI | Fórmula |
|---|---|
| Total de análises | `COUNT(análises)` |
| Usuários ativos | `COUNT(DISTINCT usuário com análise)` |
| Score geral médio | `AVG(overall_score)`, sem análises com falha |
| Score personalizado médio | `AVG(final_score)` |
| Cobertura da personalização | scores personalizados ÷ análises × 100 |
| Contraindicados | contraindicados ÷ scores personalizados × 100 |
| Tempo médio de processamento | `AVG(processing_time_ms)` |
| Taxa de sucesso | concluídas ÷ total × 100 |

A direção do desvio não diz sozinha se o resultado é bom. Tempo de processamento caindo é melhora; score personalizado caindo é neutro, porque depende do perfil de quem analisou.

---

## 7. O dashboard

Construído no AI/BI Dashboards do Databricks. Os 13 datasets leem apenas tabelas de `workspace.gold`.

| Página | O que mostra | Pergunta que responde |
|---|---|---|
| Visão geral | 6 cards com desvio, evolução diária, recomendação por tipo de pele, score por faixa etária, score por categoria, histograma e boxplot | O app está sendo usado? Que resultado as pessoas recebem? |
| Personalização | Score geral × score personalizado por produto, dispersão e tabela | Quanto a personalização muda a nota? |
| Catálogo e ingredientes | Risco de irritação, composição por categoria, efeitos por perfil e ranking de ingredientes | O que há nos produtos que o app avalia? |
| Base de usuários | Perfil das contas cadastradas e mapa do Brasil por estado | Quem são e onde estão os usuários? |
| Saúde do pipeline | Resultado da última validação cruzada | Os números são confiáveis? |

**Filtros globais:** origem do dado (padrão `real`), período, tipo de pele e categoria. Cada filtro age apenas nos datasets que têm aquele campo. O catálogo, por exemplo, não muda com o período.

**Rótulos e cores:** os códigos do banco são traduzidos no SQL dos datasets (`oily` → Oleosa, `age_18_24` → 18 a 24 anos, `contraindicated` → 5. Contraindicado), sempre com o mesmo `CASE`, para que o filtro de tipo de pele case entre os datasets. As cores seguem a paleta da marca: ameixa `#321B5D`, violeta `#580BA5`, magenta `#A21561` e lilás `#CDB3E3`, com cinza `#9A9AA2` como neutro.

---

## 8. Como montar do zero

Pré-requisitos: um workspace Databricks com Unity Catalog e o [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) autenticado.

**1. Crie os segredos.** Nenhuma credencial fica no código.

```bash
databricks secrets create-scope venus
databricks secrets put-secret venus jdbc_url      # jdbc:postgresql://<host>:<porta>/Venus-qa?sslmode=require
databricks secrets put-secret venus bi_user       # usuário somente leitura do schema venus_bi
databricks secrets put-secret venus bi_password
```

**2. Importe os notebooks.**

```bash
D=/Users/<seu-usuario>
databricks workspace import $D/Venus_Bronze    --file notebooks/01_bronze.py    --format SOURCE --language PYTHON
databricks workspace import $D/Venus_Silver    --file notebooks/02_silver.py    --format SOURCE --language PYTHON
databricks workspace import $D/Venus_Gold      --file notebooks/03_gold.py      --format SOURCE --language PYTHON
databricks workspace import $D/Venus_Catalogo  --file notebooks/04_catalogo.py  --format SOURCE --language PYTHON
databricks workspace import $D/Venus_Validacao --file notebooks/05_validacao.py --format SOURCE --language PYTHON
```

**3. Crie o Job.** Ajuste os caminhos `notebook_path` em `job/venus_bi_pipeline.json` para a sua pasta e rode:

```bash
databricks jobs create --json @job/venus_bi_pipeline.json
```

**4. Rode o pipeline uma vez** para criar as tabelas da gold: `databricks jobs run-now <job_id>`.

**5. Importe o dashboard** em **Dashboards → Import** usando `dashboard/venus_bi.lvdash.json`, escolha o SQL warehouse e publique.

---

## 9. Operação do dia a dia

| Situação | O que fazer |
|---|---|
| Atualizar os dados agora | `databricks jobs run-now <job_id>` ou **Run now** na tela do Job |
| Ver se a última carga deu certo | Página **Saúde do pipeline** do dashboard, ou o histórico em **Jobs & Pipelines → Runs** |
| Passar a usar a produção | Trocar o banco no segredo `jdbc_url` de `Venus-qa` para `Venus-2ano`. Nada no código muda |
| A equipe de banco mudou uma view | Rodar o bronze e conferir a última célula, que lista as colunas de cada tabela de uso |
| Editar um notebook | Editar no Databricks, exportar com `databricks workspace export <caminho> --format SOURCE` e abrir PR aqui |

---

## 10. Problemas conhecidos e soluções

| Sintoma | Causa | Solução |
|---|---|---|
| Bronze falha em todas as views com "database does not exist" | O PostgreSQL diferencia maiúsculas no nome do banco | Usar `Venus-qa`, com **V** maiúsculo |
| `SyntaxError: invalid non-printable character U+FEFF` | Arquivo do notebook salvo com BOM (comum no PowerShell) | Salvar em UTF-8 **sem** BOM antes de importar |
| Acentos quebrados no dashboard (`Propor├º├úo`) | JSON do dashboard enviado pelo PowerShell com a codificação do console | Enviar o arquivo direto: `databricks lakeview update <id> --json @arquivo.json`, salvo em UTF-8 |
| Validação dá `DIVERGENTE` em tudo | A bronze está desatualizada em relação ao banco | Rodar o Job inteiro, não só a validação |
| Coluna não encontrada na silver | A equipe de banco renomeou uma coluna da view (ex.: `hair_type` → `hair_pattern`) | Ajustar o nome no `02_silver.py` |
| `ruff` acusa `spark` ou `dbutils` indefinidos | São objetos injetados pelo runtime do Databricks | Já configurado em `pyproject.toml` (`builtins`) |

---

## 11. Limitações

- **Amostra de testes:** os dados vêm do QA, com poucos testadores. Conclusões por perfil descrevem esse grupo, não o público do app.
- **Volume pequeno:** com dezenas de registros por semana, os desvios dos cards viram porcentagens grandes. Ganham sentido com uso contínuo.
- **Campos incompletos no app:** a compatibilidade (%) e a nota das reviews estão pouco preenchidas e ficaram fora dos indicadores principais.
- **Carga total:** a bronze relê tudo todo dia. Quando o volume crescer, vale migrar para carga incremental.
- **Databricks Free Edition:** só computação serverless e no máximo 5 tarefas simultâneas. O pipeline cabe nesses limites.

O relatório gerencial em `docs/relatorio_gerencial_bi.html` detalha os resultados, a análise crítica e as recomendações.
