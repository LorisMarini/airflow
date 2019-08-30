Welcome to Airflow!
===============================================================================

Airflow is an open source tool to author and schedule workflows (you can think of it as a much, **much better chron**). It is often used to manage batch data transformations also known as ETL - Extract Transform Load. The [project](https://github.com/apache/airflow), was originally developed by Airbnb and is written in Python 3. The managed version in GCP is called [Cloud Composer](). Not surprisingly, simplification comes at the cost of flexibility, and there are a [number of factors](https://paper.dropbox.com/doc/Cloud-Composer--Aj2xjfXGAtMaEP~LvV4uaPzoAg-q14602O2N3PoxXP3xLnlt) to be considered.

If you are wondering what other options are out there, [this doc](https://paper.dropbox.com/doc/Tooling-Workflow-Orchestration--Aj21verpo8ROSiat0UeaGkV3Ag-i7ey1rnUSEEvIQaFAcuQz) might be a starting point.

## Principles

**Transactional**: A piece of computation should either succeed or fail. You should not provision for things like “partially succeeded”.
**Idempotent**: When you re-run the same code for the same part of the pipeline you should get ALWAYS the same result.
**Resilient**: Designed for failure. Any data comes from somewhere and things have bugs/get improved along the way.
**Zero-Administration**: Once the logic is defined, scaling out should be straightforward.

The principle of zero-administration results from a separation of concerns. The definition of tasks in Directed Acyclic Graphs (DAGs), task scheduling, and task execution, are handled by different agents. This allows the system to scale horizontally leveraging container orchestration engines such as kubernetes. To keep in mind - vanilla Airflow is a task management tool, not a resource optimisation tool. In practice this means that in single-node applications you are responsible for balancing load and optimise resource utilisation.  

## DAG Orchestration

This is where the work is broken down into atomic pieces (**tasks**), connected with each other by a set of dependencies to form a directed acyclic graph (**DAG**). In order to achieve transactional execution, it is not recommended to pass state between tasks. If your code can tolerate a machine restart at any point of the execution of a DAG, you can be reasonably sure that the code is idempotent and transactional. A typical DAG looks like this:

## Local Executors


## State
Airflow keeps the state of each DAG in a relational database (OLTP - Online Transactional Processing). Options are MySQL and PostgreSQL, although the first is not ideal in large projects for reasons related to concurrency. In this repo, I use a local PostgresQL dB, deployed and mounted via the `docker_compose.yml` file

## Scaling

When the cores on your VM are not enough to get the job done, you can scale out Airflow in at least [two ways](https://paper.dropbox.com/doc/Scaling-Out-Airflow--Aj0ISuhrbfc7CmwPNMbHyhgBAg-NFbqIBfvkevcVOj7huwT3). The first uses a distributed task queue called [Celery](http://www.celeryproject.org/), the second leverages the scalability of kubernetes(), where each task is run in an isolated pod with its own Docker image, logs, metrics, failure recovery etc.

## Secrets

This repo uses [transcrypt](https://github.com/elasticdog/transcrypt) to transparently encrypt/decrypt sensitive information with **aes-256-cbc** cypher (test password is `LorisMarini`). All the secrets needed to talk to the rest of the world (**Slack, Claudant, Google Sheet, Google BigQuery, S3, GCS,** ...) are stored in `/secrets`. During remote deployment in a node/cluster, the decryption key can be read from an authenticated bucket (e.g. Amazon S3). Transcrypt is included as a submodule, so to clone the repo use:

`git clone --recurse-submodules https://github.com/LorisMarini/helpers.git`

To setup transcrypt simple run

`decrypt-secrets.sh MYSECRET` replacing MYSECRET as appropriate. To see what is being encrypted, cd into the repo and type `git ls-crypt`. To change this, modify the file `.gitattributes`.


password `airflow`
