# Arquitetura alvo em AWS

## Visão geral

Proposta de produção para o pipeline de corridas de táxi usando uma arquitetura lakehouse em AWS.

```text
Origem pública TLC
      │
      ▼
Ingestão agendada
AWS Lambda ou ECS Fargate
      │
      ▼
Amazon S3 - Raw/Landing
      │
      ▼
AWS Glue / EMR Serverless - PySpark
      │
      ├── S3 Bronze
      ├── S3 Silver
      └── S3 Gold
             │
             ▼
AWS Glue Data Catalog + Athena / Redshift Spectrum / QuickSight
```

## Serviços

- **Ingestão:** Lambda, ECS Fargate ou Glue Python Shell para baixar os arquivos mensais.
- **Armazenamento:** Amazon S3 separado por camadas `raw`, `bronze`, `silver` e `gold`.
- **Processamento:** AWS Glue Spark ou EMR Serverless com PySpark.
- **Catálogo:** AWS Glue Data Catalog.
- **Consumo:** Athena, Redshift Spectrum ou QuickSight.
- **Orquestração:** Amazon Managed Workflows for Apache Airflow ou Step Functions.
- **Observabilidade:** CloudWatch Logs, CloudWatch Metrics, alarmes e tabela de auditoria.
- **Qualidade:** regras de validação em Spark, tabela de rejeitados e métricas por tipo de erro.

## Incrementalidade e particionamento

- Particionar viagens por `year_month` ou `pickup_date`.
- Usar controle de arquivos processados por `source_file`, `source_year_month` e `ingestion_run_id`.
- Reprocessamento por mês: apagar e recomputar apenas a partição impactada.

## Idempotência

- A camada bronze deve ser sobrescrita por partição de mês.
- A camada silver deve ser recalculada a partir da bronze.
- A camada gold deve ser recalculada a partir da silver.
- Auditorias devem guardar `run_id`, arquivo origem, quantidade lida, válidos e inválidos.

## Segurança

- S3 com criptografia SSE-S3 ou SSE-KMS.
- IAM com menor privilégio.
- Bloqueio de acesso público nos buckets.
- Segregação por ambientes: dev, hml e prd.

## Controle de custos

- Usar Glue/EMR Serverless com execução sob demanda.
- Arquivos em Parquet e particionamento para reduzir scan no Athena.
- Compactação de pequenos arquivos na silver/gold.
- Políticas de lifecycle para logs e dados temporários.
