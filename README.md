Welcome to Airflow!
===============================================================================

Airflow is an open source tool to author and schedule workflows (you can think of it as a much, **much better chron**). It is often used to manage batch data transformations also known as ETL - Extract Transform Load. The [project](https://github.com/apache/airflow), was originally developed by Airbnb and is written in Python 3. The managed version in GCP is called [Cloud Composer](). Not surprisingly, simplification comes at the cost of flexibility, and there are a [number of factors](https://paper.dropbox.com/doc/Cloud-Composer--Aj2xjfXGAtMaEP~LvV4uaPzoAg-q14602O2N3PoxXP3xLnlt) to be considered.

If you are wondering what other python-based options are available, [this doc](https://paper.dropbox.com/doc/Tooling-Workflow-Orchestration--Aj21verpo8ROSiat0UeaGkV3Ag-i7ey1rnUSEEvIQaFAcuQz) might be a starting point.

## Principles
Airflow is built around four core principles:

  + **Transactional**: A piece of computation should either succeed or fail. You should not provision for things like “partially succeeded”.
  + **Idempotent**: When you re-run the same code for the same part of the pipeline you should get ALWAYS the same result.
  + **Resilient**: Designed for failure. Any data comes from somewhere and things have bugs/get improved along the way.
  + **Zero-Administration**: Once the logic is defined, scaling out should be straightforward.

The principle of zero-administration results from a separation of concerns. The definition of tasks in Directed Acyclic Graphs (DAGs), task scheduling, and task execution, are handled by different agents. This allows the system to scale horizontally leveraging container orchestration engines such as [kubernetes](https://kubernetes.io/). To keep in mind - vanilla Airflow is a task management tool, not a resource optimisation tool. In practice this means that in single-node applications you are responsible for balancing load and optimise resource utilisation.  

## What Airflow is not

Depending on how data is collected before the transformation (T) step, ETL pipelines can be based on **batch** or **stream** processing.

**Airflow is not** a stream processing tool. Although it is possible to run batch jobs every 15 minutes, even simple aggregations over larger time windows require to write additional code and are just cumbersome. If your application requires real time processing, consider using tools like [Spark Streaming](https://spark.apache.org/streaming/), [Apache Samza](http://samza.apache.org/), or [Apache Beam](https://beam.apache.org/). There is also a managed version of the latter in GCP, under the name of [Dataflow](https://cloud.google.com/dataflow/). This doc might be a good starting point.

**Airflow is not** a language-agnostic workflow orchestration tool. It is written in python and most of the code you can run natively is python code. The bash_operator can be used to execute arbitrary code, but you still need a python environment and Airflow itself is still interpreted. If you need a language-agnostic tool, checkout [pachyderm](http://pachyderm.io/) and have a look at this [post](http://gopherdata.io/post/more_go_based_workflow_tools_in_bioinformatics/).

## DAG Orchestration

This is where the work is broken down into atomic pieces (**tasks**), connected with each other by a set of dependencies to form a directed acyclic graph (**DAG**). In order to achieve transactional execution, it is not recommended to pass state between tasks. If your code can tolerate a machine restart at any point of the execution of a DAG, you can be reasonably sure that the code is idempotent and transactional. A task is an instance of one of the many Airflow [operators](https://airflow.readthedocs.io/en/stable/_api/airflow/operators/index.html), most notably the bash_operator to run arbitrary commands, the PythonOperator to interpret and execute python code.

## State
Airflow keeps the state of each DAG in a relational database (OLTP - Online Transactional Processing). Options are MySQL and PostgreSQL, although the first is not ideal in large projects for reasons related to concurrency. In this repo, I use a local PostgresQL dB, deployed and mounted via the `docker_compose.yml` file.

## Single-Node Execution
There are two executors available:

  + **SequentialExecutor**: One task at a time.
  + **LocalExecutor**: Concurrent task execution.

## Multi-Node Execution

When the cores on your VM are not enough to get the job done, you can scale out Airflow in at least [two ways](https://paper.dropbox.com/doc/Scaling-Out-Airflow--Aj0ISuhrbfc7CmwPNMbHyhgBAg-NFbqIBfvkevcVOj7huwT3). The first uses a distributed task queue called [Celery](http://www.celeryproject.org/), the second leverages the scalability of kubernetes with the [KubernetesPodOperator](https://airflow.readthedocs.io/en/stable/kubernetes.html). In this second case, each task runs in an isolated k8s pod with its own Docker image, logs, metrics and replay logic, for ultimate flexibility and scalability. The scheduler and webserver deamons also run inside two separate pods. Just like all distributed systems though, consistency between environments of different pods can be tricky, but the problem [has been solved](https://www.youtube.com/watch?v=A0gKV1r7w8M&feature=youtu.be).

## Airflow Configuration

Inside `airflow.cfg` or via environment variables. Variables should be named as follows:

    AIRFLOW__X__Y

where X is the section of the config file in capital letters (e.g. CORE), Y is the particular parameter in capital letters (e.g. REMOTE_LOGGING), and __ is a double underscore. So to setup the SQLAlchemy string to talk to your dB of choice, the name of the env variable will be **AIRFLOW__CORE__SQL_ALCHEMY_CONN**.

## Repo secrets

This repo uses [transcrypt](https://github.com/elasticdog/transcrypt) to transparently encrypt/decrypt the `/secrets` directory  with **aes-256-cbc** cypher (test password is `airflow`). During remote deployment in a node/cluster, the decryption key can be read from an authenticated bucket (e.g. Amazon S3). Transcrypt is included as a submodule, so to clone the repo use:

`git clone --recurse-submodules https://github.com/LorisMarini/helpers.git`

To setup transcrypt simple run

`decrypt-secrets.sh MYSECRET` replacing MYSECRET as appropriate. To see what is being encrypted, cd into the repo and type `git ls-crypt`. To change this, modify the file `.gitattributes`.
