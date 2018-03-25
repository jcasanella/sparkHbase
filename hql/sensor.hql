drop table if exists sensor;

create external table if not exists sensor (
    resid String,
    date String,
    time String,
    hz Double,
    disp Double,
    flo Double,
    sedPPM Double,
    psi Double,
    chlPPM Double
) stored as parquet
location 'hdfs://quickstart.cloudera:8020/user/cloudera/data_parquet';