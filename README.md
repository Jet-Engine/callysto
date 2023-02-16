<h1 align="center"><img src="art/callysto_banner_2000x500.png"/></h1>
<div align="center">
 <strong>
    Callysto is stream processing framework for Rust with focus on performance and durability.
 </strong>
</div>

<br />

**NOTE: Callysto is used in production environment of multiple companies and it is production ready. This readme will be updated with more information about usage of Callysto.**

### Requirements

* cmake
* clang
* libtool
* libstdc++-devel

### Rust MSRV

We are using:

```shell
cargo 1.60.0 (d1fd9fe 2022-03-01)
```

### Running

#### With Docker Compose - Nerdctl Compose

1. You need to have docker installation locally.
With containerd backend, if you want to bring single node Kafka replacement use:

```shell
nerdctl compose up
```

If you want to bring Confluent Kafka single node. You can:

```shell
nerdctl compose -f docker-compose.kafkasn.yml
```

If you want to bring full blown Confluent Kafka cluster. Use:

```shell
nerdctl compose -f docker-compose.kafkacluster.yml
```

Mind that `nerdctl` command is interchangeable with `docker`.

#### With K8S

1. You need to have K8S installation locally.

```shell
cd k8s && kubectl apply -f . && kubectl port-forward svc/redpanda 9092:9092
```

3. Now you can run the producers in Python.

```shell
virtualenv venv
source venv/bin/activate
which pip3 # check that it is pointing to venv
pip3 install aiokafka -U
python examples/producer.py
```

3. Now you can spawn the double agent example (without durability).

```shell
RUST_LOG="info,rdkafka::client=warn" RUST_BACKTRACE=full cargo run --example double-agent
```

Environment variables passed above is optional, but suggested for development.
