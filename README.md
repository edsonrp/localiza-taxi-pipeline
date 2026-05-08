# Localiza&Co — Desafio Técnico Engenharia de Dados Sênior

Projeto local em **PySpark** para ingestão, tratamento, modelagem analítica e documentação arquitetural usando os dados públicos de **NYC Green Taxi Trip Records** e **Taxi Zone Lookup**.

## Objetivo

Construir um pipeline analítico reproduzível localmente, com separação de camadas, auditoria de ingestão, regras de qualidade, saídas analíticas e proposta de arquitetura cloud para produção.

## Stack escolhida

- Python 3.10 ou 3.11
- PySpark local
- Parquet como formato de armazenamento local
- Makefile para execução simples
- Estrutura em camadas: landing, bronze, silver e gold

> Observação: para execução local com PySpark, é necessário ter Java instalado e a variável `JAVA_HOME` configurada.

## Estrutura do projeto

```text
localiza-taxi-pipeline/
├── .github/workflows/ci.yml
├── configs/
│   └── local.yaml
├── data/
│   ├── landing/             # arquivos baixados da origem; não versionar
│   ├── bronze/              # dados brutos padronizados; não versionar
│   ├── silver/              # dados tratados e validados; não versionar
│   ├── gold/                # saídas analíticas; não versionar
│   └── audit/               # métricas de auditoria; não versionar
├── docs/
│   ├── architecture.md
│   └── presentation_outline.md
├── src/
│   └── taxi_pipeline/
│       ├── __init__.py
│       ├── settings.py
│       ├── spark.py
│       ├── jobs/
│       │   ├── download_data.py
│       │   ├── ingest_raw.py
│       │   ├── build_silver.py
│       │   └── build_gold.py
│       └── utils/
│           └── filesystem.py
├── tests/
│   └── test_project_structure.py
├── .gitignore
├── Makefile
├── pyproject.toml
└── requirements.txt
```

## Como executar localmente

### 1. Criar ambiente virtual

```bash
python -m venv .venv
source .venv/bin/activate
```

No Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Baixar os dados de origem

```bash
make download
```

Esse comando baixa:

- `green_tripdata_2026-01.parquet`
- `green_tripdata_2026-02.parquet`
- `green_tripdata_2026-03.parquet`
- `taxi_zone_lookup.csv`

### 4. Executar a primeira etapa: ingestão para a camada bronze

```bash
make ingest
```

A ingestão faz:

- leitura dos arquivos Parquet e CSV da camada `data/landing/`
- inclusão de metadados técnicos de ingestão
- escrita dos dados brutos padronizados em `data/bronze/`
- geração de auditoria com quantidade de registros lidos por arquivo em `data/audit/ingestion_counts/`

### 5. Rodar testes simples de estrutura

```bash
make test
```

## Comandos disponíveis

```bash
make setup       # cria ambiente virtual
make install     # instala dependências
make download    # baixa arquivos públicos
make ingest      # executa ingestão bronze
make test        # roda testes básicos
make clean-data  # remove data/bronze, data/silver, data/gold e data/audit
```

## Próximas etapas da implementação

1. Criar camada silver com regras de qualidade:
   - datas de pickup/dropoff válidas
   - `total_amount > 0`
   - `trip_distance >= 0`
   - `PULocationID` e `DOLocationID` não nulos
   - segregação de registros inválidos por tipo de erro
   - deduplicação por chave técnica de viagem
2. Criar camada gold com as três saídas obrigatórias:
   - top 10 zonas por receita
   - 5 corridas mais caras considerando a corrida mais cara de cada dia
   - gorjeta média por borough de desembarque para pagamento em cartão
3. Completar documentação de arquitetura AWS.
```
