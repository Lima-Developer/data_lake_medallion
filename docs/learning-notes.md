# Notas de Aprendizado - Data Lake

Documentação das decisões técnicas, explicações detalhadas e anotações de aprendizado durante a construção deste projeto.

---

## Estrutura de Pastas

| Pasta | Propósito |
|---|---|
| `dags/` | Arquivos de orquestração do Airflow |
| `spark/` | Jobs PySpark |
| `dbt/` | Modelos de transformação |
| `data/raw/` | Arquivos CSV fonte |
| `docker/airflow/` | Dockerfile customizado do Airflow |

---

## Docker Compose

### Extensões do Compose

No Docker Compose, em versões da série 3.x temos uma funcionalidade chamada **extensão**.

Com a utilização do prefixo `x-` definimos um campo de extensão, que permite definir valores comuns no topo do nosso arquivo que podem ser referenciados posteriormente, ajudando a manter o arquivo mais eficiente e fácil de manter.

Qualquer campo que comece com `x-` é considerado uma extensão. O Docker Compose ignora esses campos ao processar os serviços, tornando-os seguros para adicionar metadados personalizados.

Geralmente eles são combinados com **âncoras YAML** (`&`) e **aliases** (`*`) para evitar a repetição de configurações.

No nosso projeto iremos fazer a utilização dessa combinação reutilizando esse template para os serviços do Airflow.

---

### Airflow Common

```yaml
x-airflow-common: &airflow-common
  build:
    context: ./docker/airflow
    dockerfile: Dockerfile
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    AIRFLOW__CORE_FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE_LOAD_EXAMPLES: 'false'
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    AWS_ACCESS_KEY_ID: test
    AWS_SECRET_ACCESS_KEY: test
    AWS_DEFAULT_REGION: us-east-1
    SPARK_HOME: /opt/spark
  volumes:
    - ./dags:/opt/airflow/dags
    - ./spark:/opt/airflow/spark
    - ./dbt/lake:/opt/airflow/dbt
    - ./data:/opt/airflow/data
    - airflow-logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy
    localstack:
      condition: service_healthy
  networks:
    - lake
```

Na declaração desse Commons do Airflow, os pontos mais importantes para o nosso projeto são os seguintes parâmetros:

- **`AIRFLOW__CORE__EXECUTOR:`** Aqui configuramos o Airflow para utilizar o `LocalExecutor`, que irá executar tasks em processos locais, sem a necessidade de Celery/Redis.
- **`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:`** Essa é a string de conexão do banco de metadados do próprio Airflow. Conecta ao PostgreSQL no host postgres, porta 5432, com usuário e senha airflow.
- **`AIRFLOW__CORE_FERNET_KEY:`** Definição da chave de criptografia. Como esse projeto é apenas para prática dos conceitos teóricos de engenharia de dados, iremos manter vazio.
- **`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION:`** DAGs recém-criadas iniciam sempre pausadas. Isso força que elas sejam ativadas manualmente na UI.
- **`AIRFLOW__CORE_LOAD_EXAMPLES:`** Setamos como false para desabilitar o carregamento das DAGs de exemplo que já vêm com o Airflow.
- **`AIRFLOW__WEBSERVER__EXPOSE_CONFIG:`** Setamos como true para permitir visualizar essas configurações completas do Airflow pela sua interface web, que por motivos de um projeto prático será útil para aprendizado.
- **`SPARK_HOME:`** Aqui definimos o caminho do Spark dentro do container do Airflow. Importante pois será utilizado para submeter jobs Spark.
- **`volumes:`** Na seção de volumes, apenas estamos montando as pastas locais dentro do container para acesso pelo Airflow.

---

## Services

### LocalStack

Estamos utilizando uma imagem oficial do LocalStack para emular os serviços da AWS localmente.

A importância do LocalStack está exatamente na nossa camada de Arquitetura Medalhão com os buckets separados (bronze, silver, gold).

```yaml
ports:
  - "4566:4566"
environment:
  - SERVICES=s3
  - DEBUG=0
  - PERSISTENCE=1
```

- **`Ports:`** Expondo a porta 4566 para comunicação no host.
- **`SERVICES=s3:`** Especifica quais serviços iremos utilizar (no nosso caso apenas o S3).
- **`DEBUG=0:`** Desabilita logs de debug.
- **`PERSISTENCE=1:`** Habilita a persistência — os dados do S3 irão sobreviver a reinicialização do container.

#### Healthcheck

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
  interval: 30s
  timeout: 5s
  retries: 5
```

Aqui verificamos a saúde do LocalStack a cada 30 segundos, com timeout de 5 segundos e até 5 tentativas. Ele utiliza o endpoint de health interno para fazer essa verificação de saúde do serviço.

---

### Spark Master

Definição do Nó Master do Spark, responsável por gerenciar os recursos do cluster. Ele:

- Recebe pedidos de execução de aplicações (spark-submit)
- Sabe quais workers estão disponíveis e a quantidade de recursos disponíveis (memória, cores)
- Decide onde alocar os executores para cada aplicação
- Expõe a UI web (porta 8080) para acompanhamento das execuções

```yaml
spark-master:
  image: apache/spark:3.5.8-python3
  container_name: spark-master
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
  ports:
    - "8081:8080"
    - "7077:7077"
  networks:
    - lake
```

---

### Spark Worker

Definição do nó Worker (Compute) com as seguintes responsabilidades:

- Registra-se no master oferecendo seus recursos (No nosso caso: 1GB RAM, 2 cores)
- Recebe tasks do driver (Leitura de CSV, transformações, escritas no Lake)
- Executa as tasks em paralelo usando os cores disponíveis
- Reporta resultados de volta ao driver

```yaml
spark-worker:
  image: apache/spark:3.5.8-python3
  container_name: spark-worker
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
  environment:
    - SPARK_WORKER_MEMORY=1g
    - SPARK_WORKER_CORES=2
  volumes:
    - ./data:/opt/airflow/data
    - ./docker/airflow/hadoop-aws-3.3.4.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar
    - ./docker/airflow/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
  depends_on:
    - spark-master
  networks:
    - lake
```

Estamos passando os JARs no volume do container pois precisamos deles para operações S3 que rodarão no worker.

> O **Spark 3.5.8** com Hadoop 3 utiliza o `hadoop-client-api` (um uber-JAR) + `hadoop-client-runtime`. Mas o suporte a S3 (`hadoop-aws`) não vem incluído pois é um módulo opcional. E `hadoop-aws` depende do `aws-java-sdk-bundle` para a comunicação via HTTP com a API do S3.

---

### PostgreSQL

O Airflow precisa de um banco de dados para salvar os metadados e poder rastrear as execuções das DAGs, os estados das tasks e as conexões.

```yaml
postgres:
  image: postgres:16
  container_name: postgres
  environment:
    - POSTGRES_USER=airflow
    - POSTGRES_PASSWORD=airflow
    - POSTGRES_DB=airflow
  volumes:
    - postgres-data:/var/lib/postgresql/data
  healthcheck:
    test: ["CMD", "pg_isready", "-U", "airflow"]
    interval: 5s
    retries: 5
  networks:
    - lake
```

---

### Airflow (Init, Webserver, Scheduler)

```yaml
airflow-init:
  <<: *airflow-common
  container_name: airflow_init
  entrypoint: /bin/bash
  command: 
  - -c
  - |
    airflow db migrate
    airflow users create \
      --username airflow \
      --password airflow \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com
  environment:
    <<: *airflow-common-env
  restart: "no"

airflow-webserver:
  <<: *airflow-common
  container_name: airflow-webserver
  command: webserver
  ports:
    - "8080:8080"
  healthcheck:
    test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
    interval: 10s
    timeout: 10s
    retries: 5
  restart: always

airflow-scheduler:
  <<: *airflow-common
  container_name: airflow-scheduler
  command: scheduler
  restart: always
```

---

## Comandos Úteis

### Comunicação com o LocalStack

```shell
aws --endpoint-url=http://localhost:4566 {comandos AWS CLI}
```

### Spark Submit ao Driver (Airflow-webserver)

```shell
docker exec airflow-webserver spark-submit \
  --master spark://spark-master:7077 \
  /opt/airflow/spark/{nome do arquivo}
```

### dbt Build and Test Models

```shell
docker exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir ."
```
