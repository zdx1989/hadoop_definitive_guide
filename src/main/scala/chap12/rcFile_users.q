drop table if exists rf_users;

create table if not exists rf_users (
    id int,
    name string
)
row format serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
stored as rcFile;

insert overwrite table rf_users
select * from users;