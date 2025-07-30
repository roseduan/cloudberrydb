CREATE EXTENSION postgres_fdw;

CREATE TABLE remote_table (
  id int,
  data text
) DISTRIBUTED BY (id);

CREATE TABLE remote_types (
  id int,
  val_text text,
  val_bool boolean,
  val_float float,
  val_date date
) DISTRIBUTED BY (id);

CREATE TABLE remote_mismatch (
  data text,
  id int
) DISTRIBUTED BY (id);

CREATE TABLE remote_unique (
  id int PRIMARY KEY,
  data text
) DISTRIBUTED BY (id);

CREATE TABLE remote_extra (
  id int,
  data text,
  extra text
) DISTRIBUTED BY (id);

CREATE TABLE remote_swap (
  id int,
  data text
) DISTRIBUTED BY (id);

CREATE TABLE remote_notnull (
  id int NOT NULL,
  data text NOT NULL
) DISTRIBUTED BY (id);

CREATE TABLE remote_with_default (
  id int,
  data text DEFAULT 'default-from-remote'
) DISTRIBUTED BY (id);

CREATE TABLE remote_text_types (
  id int,
  val_char char(5),
  val_varchar varchar(20)
) DISTRIBUTED BY (id);

CREATE TABLE remote_binary (
  id int,
  data bytea
) DISTRIBUTED BY (id);

CREATE TABLE remote_partitioned (
  id int,
  data text
) PARTITION BY RANGE (id) DISTRIBUTED BY (id);

CREATE TABLE remote_partition_1 PARTITION OF remote_partitioned
  FOR VALUES FROM (1) TO (10000);

CREATE TABLE remote_partition_2 PARTITION OF remote_partitioned
  FOR VALUES FROM (10000) TO (20000);

CREATE TABLE remote_partition_default PARTITION OF remote_partitioned
  DEFAULT;
