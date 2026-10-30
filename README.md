# Venus BI

Camada de Business Intelligence do **Venus**, recomendador personalizado de cosméticos (Projeto Interdisciplinar, 2º ano).
Este repositório guarda tudo o que roda no Databricks: os notebooks do pipeline, a definição do Job, o dashboard e o relatório gerencial.

## Visão geral

```
PostgreSQL (Aiven)              Databricks (Free Edition, serverless)
banco Venus-qa                  ┌──────────┐   ┌──────────┐   ┌──────────┐
schema venus_bi  ──JDBC──►      │  bronze  │──►│  silver  │──►│   gold   │──► Dashboard AI/BI
(14 views, modelo estrela)      │ ingestão │   │tratamento│   │   KPIs   │    (5 páginas)
                                └──────────┘   └────┬─────┘   └────┬─────┘
                                                    └──► catálogo ─┘
                                                              │
                                                    validação cruzada com o Postgres
```

| Camada | Schema | O que faz |
|---|---|---|
| Bronze | `workspace.bronze` | Cópia fiel das 14 views do `venus_bi`, com a data e a hora da carga |
| Silver | `workspace.silver` | Remove duplicatas e dados pessoais (LGPD), padroniza campos vazios e junta fatos com usuário, produto e data |
| Gold | `workspace.gold` | KPIs com desvio (7 dias × 7 dias anteriores), evolução diária, perfil × recomendação, catálogo e efeito da personalização |

## Estrutura

```
notebooks/
  01_bronze.py        ingestão via JDBC (Venus_Bronze)
  02_silver.py        tratamento e LGPD (Venus_Silver)
  03_gold.py          KPIs de uso e personalização (Venus_Gold)
  04_catalogo.py      catálogo de ingredientes e efeito da personalização (Venus_Catalogo)
  05_validacao.py     compara a gold com as views analíticas do Postgres (Venus_Validacao)
job/
  venus_bi_pipeline.json   configuração do Job "Venus BI · pipeline diário"
dashboard/
  venus_bi.lvdash.json     definição do dashboard AI/BI (13 datasets, 5 páginas + filtros)
docs/
  relatorio_gerencial_bi.html   relatório gerencial (abrir no navegador)
```

Os notebooks estão no formato *source* do Databricks (`.py` com `# COMMAND ----------` separando as células).
Para importar: **Workspace → Import → File**, ou `databricks workspace import <destino> --file notebooks/01_bronze.py --format SOURCE --language PYTHON`.

## Job

- Nome: **Venus BI · pipeline diário**
- Agendamento: todo dia às 04:00 (America/Sao_Paulo)
- Tarefas: `venusbronze` → `venussilver` → (`venusgold` ∥ `venuscatalogo`) → `venusvalidacao`
- Cada tarefa só roda se as anteriores terminarem com sucesso. Se uma falha, as tabelas finais ficam como estavam, e o dashboard nunca mostra dados pela metade.

Os caminhos dos notebooks em `job/venus_bi_pipeline.json` apontam para a pasta de usuário onde o pipeline foi criado. Ajuste-os ao recriar o Job em outro workspace.

## Configuração

Nenhuma credencial fica no código. Antes de rodar, crie o escopo de segredos `venus` com três chaves:

```bash
databricks secrets create-scope venus
databricks secrets put-secret venus jdbc_url      # jdbc:postgresql://<host>:<porta>/Venus-qa?sslmode=require
databricks secrets put-secret venus bi_user       # usuário somente leitura do schema venus_bi
databricks secrets put-secret venus bi_password
```

Para apontar para a produção quando o app for lançado, basta trocar o banco no segredo `jdbc_url` (`Venus-qa` → `Venus-2ano`). O PostgreSQL diferencia maiúsculas no nome do banco.

## Dashboard

O arquivo `dashboard/venus_bi.lvdash.json` pode ser importado em **Dashboards → Import**. As consultas leem apenas tabelas de `workspace.gold`.

| Página | Conteúdo |
|---|---|
| Visão geral | 6 cards com desvio, evolução diária, recomendação por tipo de pele, histograma e boxplot |
| Personalização | Score geral × score personalizado por produto |
| Catálogo e ingredientes | Risco de irritação, composição e efeitos por perfil |
| Base de usuários | Perfil das contas cadastradas |
| Saúde do pipeline | Resultado da última validação cruzada |

Filtros globais: origem do dado, período, tipo de pele e categoria.

## Relatório gerencial

`docs/relatorio_gerencial_bi.html` apresenta a base, o problema analisado, as estratégias de tratamento, os KPIs, os resultados, o pipeline e as evidências do Job.
