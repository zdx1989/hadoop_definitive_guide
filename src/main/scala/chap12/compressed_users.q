drop table if exists compressed_users;

create table if not exists compressed_users (
    id int,
    name string
)
row format delimited
    fields terminated by '\t'
    lines terminated by '\n'
stored as sequenceFile;

set hive.exec.compress.output=true;
set mapred.output.compress=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

insert overwrite table compressed_users
select * from users;