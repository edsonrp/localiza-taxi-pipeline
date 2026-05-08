# Roteiro da apresentação — 20 a 30 minutos

## 1. Contexto do desafio — 2 min

- Pipeline analítico de corridas de táxi.
- Dados públicos TLC Green Taxi + Taxi Zone Lookup.
- Objetivo: ingestão, qualidade, modelagem, saídas analíticas e arquitetura cloud.

## 2. Decisão técnica — 4 min

- Escolha de PySpark local.
- Por que Spark: processamento distribuído, aderência ao mundo cloud, leitura nativa de Parquet e bom encaixe com Glue/EMR.
- Por que não usar infraestrutura cloud no case: requisito de reprodutibilidade local.

## 3. Organização do projeto — 4 min

- Separação por camadas: landing, bronze, silver e gold.
- Configuração centralizada em YAML.
- Jobs separados por responsabilidade.
- Makefile para execução simples.

## 4. Qualidade e auditoria — 5 min

- Métricas de leitura por arquivo.
- Contagem de válidos e inválidos.
- Rejeição por tipo de erro.
- Deduplicação.

## 5. Saídas analíticas — 5 min

- Receita por zona de embarque.
- Corridas mais caras do período.
- Gorjeta média por borough de desembarque.

## 6. Arquitetura AWS alvo — 5 a 8 min

- S3, Glue/EMR Serverless, Glue Data Catalog, Athena/QuickSight.
- Orquestração com MWAA ou Step Functions.
- Observabilidade com CloudWatch.
- Incrementalidade, idempotência, segurança e FinOps.

## 7. Trade-offs e evoluções — 3 min

- Evoluir para Iceberg/Delta.
- Data quality com Great Expectations ou Deequ.
- CI/CD com deploy de jobs.
- Testes unitários e testes de contrato de schema.
