# Project14

## Description
This is a part of course CS523 (MUM).

Analyze tweets sentiment by using Spark streaming and making chart in near real-time.

# Architecture
Get streaming data from twitter, then push message into Kafka. Spark streaming app process raw data from kafka, then push processed data into kafka. Web app read result from kafka and showing chart on browser.
(For demonstration purpose, so i used 1 Spark master, 1 Spark worker, Zookeeper, Kafka run as single mode) \
![Architecture](./image/architecture.jpeg "Architecture")

# Packages info
- Scala 2.12
- Kafka 1.0.1
- Spark 2.3.0
- NodeJs 8.10
- AFINN 0.1 (python package)
- Plotly 2.5

## Prerequisites
- Java 8
- Python 3

## Kafka
- Download packge
```shell
wget http://apache.claz.org/kafka/1.0.1/kafka_2.12-1.0.1.tgz
tar xvfz kafka_2.12-1.0.1.tgz
cd kafka_2.12-1.0.1
# Because i run kafka on t2.micro instance, so need to limit memory usage
export KAFKA_HEAP_OPTS="-Xmx500M -Xms500M"
# Clear cache memory
sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches'
sudo sh -c 'echo 2 >/proc/sys/vm/drop_caches'
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
```

- Start Zookeeper
```shell
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/zookeeper-logs &
```

- Start Kafka
```shell
nohup bin/kafka-server-start.sh config/server.properties > ~/kafka-logs &
```

- Stop Zookeeper and Kafka
```shell
./bin/kafka-server-stop.sh
./bin/zookeeper-server-stop.sh
```

## Spark
- Download package
```shell
wget http://apache.claz.org/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
tar xvfz spark-2.3.0-bin-hadoop2.7.tgz
```

- Setup environment
```shell
# add the end of file ~/.bashrc
export SPARK_HOME=~/spark-2.2.1-bin-hadoop2.7
export PATH=$SPARK_HOME/bin:$PATH

source ~/.bashrc
```
- Start server
```shell
./sbin/start-master.sh
```

- Start worker
```shell
./sbin/start-slave.sh spark://localhost:7077
```

- Stop master and worker
```shell
./sbin/stop-slave.sh
./sbin/stop-master.sh
```
## Crawler
- Setup environment
```shell
sudo yum install python36
sudo pip-3.6 install pyspark
```

- Edit file config.py in crawler
```python
CONSUMER_KEY    = 'XXX'
CONSUMER_SECRET = 'XXX'
ACCESS_TOKEN    = 'XXX'
ACCESS_SECRET   = 'XXX'
```

- Start crawler
```shell
python tw_crwl_to_kfk.py <keyword>
```

## Dashboard
- Setup environment
```shell
sudo yum install gcc-c++ -y
curl --silent --location https://rpm.nodesource.com/setup_8.x | sudo bash -

cd project14/subcriber
npm install
```
- Start Dashboard web app
```shell
node app.js
```

## Spark Streaming app
- Setup
```shell
pip-3.6 install afinn tweepy pykafka
```

- Run spark streaming app
```shell
PYSPARK_PYTHON=python3 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 twitter_sentiment.py
```

