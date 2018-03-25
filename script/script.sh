#!/usr/bin/env bash
spark2-submit --class com.learn.streaming.parquet.StreamParquetApp --master yarn --deploy-mode client streaming_hbase.jar