CREATE SCHEMA IF NOT EXISTS mdtc_tpcc
WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE mdtc_tpcc;

CREATE TABLE RESULT (
  dc_no INT,
  view_length INT,
  exe_delay INT,
  type INT, // (0:100%read-only, 1:100%write-only, 2:20%read+80%write, 3:80%read+20%write. Default:2)
  num_clients INT,
  two_pl Boolean,
  succeed_txns INT,
  aborted_txns INT,
  num_cql_read INT,
  num_cql_write INT,
  benchmark_time BIGINT,
  throughput DOUBLE,
  cql_throughput DOUBLE,
  PRIMARY KEY(dc_no, view_length, exe_delay, type, num_clients, two_pl)
);