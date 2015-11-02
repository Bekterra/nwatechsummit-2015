impala-shell -q "create EXTERNAL TABLE meetup_parquet_snappy LIKE PARQUET '/user/ubuntu/meetup_parquet_snappy/_metadata' STORED AS PARQUET LOCATION '/user/ubuntu/meetup_parquet_snappy';"
