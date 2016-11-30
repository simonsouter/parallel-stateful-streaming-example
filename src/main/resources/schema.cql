CREATE KEYSPACE meter WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1'};

CREATE TABLE meter.total_usage (
  topic text,
  partition int,
  offset bigint static,
  meter_id int,
  samples int,
  total int,
  PRIMARY KEY ((topic, partition), meter_id)
);
