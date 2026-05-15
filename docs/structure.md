# Estrutura do Projeto - Data Lake

## Visão Geral

Este projeto implementa um Data Lake local seguindo a arquitetura **Medallion** (Bronze → Silver → Gold), orquestrado com Apache Airflow e executado inteiramente via Docker Compose.

### Stack Tecnológica

| Componente | Tecnologia | Versão |
|---|---|---|
| Orquestração | Apache Airflow | 2.11.2 |
| Ingestão | Apache Spark (PySpark) | 3.5.8 |
| Transformação | dbt-core + dbt-duckdb | 1.9.4 / 1.10.1 |
| Armazenamento | LocalStack (S3) | 3.8 |
| Banco Analítico | DuckDB | via dbt-duckdb |
| Metadados Airflow | PostgreSQL | 16 |
| Containerização | Docker Compose | - |

---

## Estrutura de Diretórios

```
DataLake/
├── dags/                          # DAGs do Airflow
│   └── lake_pipeline.py           # Pipeline principal (Medallion E2E)
├── data/
│   └── raw/                       # Dados brutos de entrada
│       ├── customers.csv
│       └── transactions.csv
├── dbt/
│   └── lake/                      # Projeto dbt (Silver + Gold)
│       ├── dbt_project.yml        # Configuração do projeto dbt
│       ├── profiles.yml           # Conexão com DuckDB + S3
│       ├── models/
│       │   ├── sources.yml        # Definição das fontes Bronze
│       │   ├── silver/            # Camada Silver (staging)
│       │   │   ├── stg_customers.sql
│       │   │   ├── stg_transactions.sql
│       │   │   └── schema.yml
│       │   └── gold/              # Camada Gold (analytics)
│       │       ├── gold_customer_summary.sql
│       │       ├── gold_revenue_by_category.sql
│       │       ├── gold_top_products.sql
│       │       └── schema.yml
│       └── lake.duckdb            # Banco DuckDB gerado
├── docker/
│   └── airflow/
│       ├── Dockerfile             # Imagem customizada do Airflow
│       ├── requirements.txt       # Dependências Python
│       ├── hadoop-aws-3.3.4.jar   # JAR para conectividade S3
│       └── aws-java-sdk-bundle-1.12.262.jar
├── docs/
│   ├── images/
│   │   └── Data_Lake_Architecture.png
│   └── structure.md               # Este arquivo
├── spark/
│   └── bronze_ingestion.py        # Job PySpark de ingestão Bronze
├── docker-compose.yml             # Infraestrutura completa
└── README.md
```

---

## Arquitetura Medallion

### Bronze (Raw → Parquet)

- **Responsável:** PySpark (`spark/bronze_ingestion.py`)
- **Entrada:** Arquivos CSV em `data/raw/`
- **Saída:** Parquet particionado em `s3://ingestion/bronze/`
- **Metadados adicionados:** `_ingested_at`, `_source_file`, `_batch_id`
- **Tabelas:** `transactions` (particionada por `order_date`), `customers`

### Silver (Cleaned & Typed)

- **Responsável:** dbt (`models/silver/`)
- **Entrada:** Parquet da camada Bronze via `read_parquet()` no DuckDB
- **Transformações:**
  - Limpeza de espaços e normalização de casing
  - Tipagem explícita de colunas
  - Remoção de registros com chaves nulas
  - Cálculo de `total_amount` em transações
- **Modelos:**
  - `stg_customers` — dados de clientes limpos
  - `stg_transactions` — transações tipadas e validadas
- **Testes:** `not_null`, `unique`, `accepted_values` (status)

### Gold (Business Analytics)

- **Responsável:** dbt (`models/gold/`)
- **Entrada:** Modelos Silver via `{{ ref() }}`
- **Modelos:**
  - `gold_customer_summary` — métricas de lifetime value por cliente
  - `gold_revenue_by_category` — receita agregada por categoria de produto
  - `gold_top_products` — ranking de produtos por receita e quantidade vendida
- **Testes:** `not_null`, `unique` nas chaves e métricas principais

---

## Pipeline (DAG)

O DAG `lake_medallion_pipeline` executa as seguintes etapas em sequência:

```
create_s3_bucket → bronze_spark_ingestion → dbt_run_silver_gold → dbt_test_quality → dbt_docs_generate
```

| Task | Tipo | Descrição |
|---|---|---|
| `create_s3_bucket` | Python (TaskFlow) | Cria o bucket `ingestion` no LocalStack |
| `bronze_spark_ingestion` | SparkSubmitOperator | Submete o job PySpark ao cluster Spark |
| `dbt_run_silver_gold` | BashOperator | Executa `dbt run` (Silver + Gold) |
| `dbt_test_quality` | BashOperator | Executa `dbt test` para validação de qualidade |
| `dbt_docs_generate` | BashOperator | Gera documentação e grafo de linhagem |

---

## Infraestrutura (Docker Compose)

### Serviços

| Serviço | Porta | Função |
|---|---|---|
| `localstack` | 4566 | Emulação de S3 (armazenamento Bronze) |
| `spark-master` | 8081, 7077 | Master do cluster Spark |
| `spark-worker` | - | Worker Spark (1GB RAM, 2 cores) |
| `postgres` | 5432 | Banco de metadados do Airflow |
| `airflow-webserver` | 8080 | Interface web do Airflow |
| `airflow-scheduler` | - | Scheduler do Airflow |
| `airflow-init` | - | Migração do banco e criação do usuário admin |

### Rede

Todos os serviços compartilham a rede bridge `lake`.

### Volumes Persistentes

- `localstack-data` — dados do S3 emulado
- `postgres-data` — metadados do Airflow
- `airflow-logs` — logs de execução das tasks

### Volumes Montados

| Host | Container | Propósito |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | DAGs do Airflow |
| `./spark` | `/opt/airflow/spark` | Jobs PySpark |
| `./dbt/lake` | `/opt/airflow/dbt` | Projeto dbt |
| `./data` | `/opt/airflow/data` | Dados brutos |

---

## Credenciais Locais

| Serviço | Usuário | Senha |
|---|---|---|
| Airflow UI | `airflow` | `airflow` |
| PostgreSQL | `airflow` | `airflow` |
| LocalStack (S3) | `test` | `test` |

---

## Como Executar

```bash
# Subir toda a infraestrutura
docker compose up -d

# Acessar o Airflow
# http://localhost:8080 (airflow/airflow)

# Acessar o Spark Master UI
# http://localhost:8081

# Ativar e executar o DAG via UI ou CLI
docker compose exec airflow-webserver airflow dags unpause lake_medallion_pipeline
docker compose exec airflow-webserver airflow dags trigger lake_medallion_pipeline
```
