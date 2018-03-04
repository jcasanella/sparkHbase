# Spark - hBase

This project shows how to read/write into hBase using Spark.

## Technologies involved

The project has been done with **sbt** instead of **maven**

1. The hBase connection is done using the newAPI from  Spark2
2. There're 2 examples how to write into into hBase

* Using the hBase API
* With the new spark connection

3. An example how to filter using the rowkey

In the future will add the following features:

1. Kafka reader / producer
* Checkpoints in case of failure
2. Spark streaming reading from kafka and writing in hBas
3. hBase connector
4. Add a cache - redis vs rocksdb

## How to check the results from hBase

1. Create an hbase table

```
create 'sensor', {NAME=>'data'}, {NAME=>'alert'}, {NAME=>'stats'}
```

2. How to check the content of a table

```
scan 'sensor', {LIMIT => 5}
```

3. How to get a specific record

```
get 'sensor', '1', 'colFamily:qualifier'
```

