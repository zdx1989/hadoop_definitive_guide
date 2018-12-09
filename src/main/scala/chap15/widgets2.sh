 sqoop import \
 --connect jdbc:mysql://localhost/hadoopguide \
 --username root \
 --password zdx1989 \
 --table widgets \
 --class-name WidgetHolder \
 --as-sequencefile \
 --target-dir widget_sequence_files \
 --bindir .

create table if not exists widgets2(
    id int,
    widget_name varchar(100),
    price double,
    design date,
    version int,
    notes varchar(200)
);

sqoop export \
--connect jdbc:mysql://localhost/hadoopguide \
--username root \
--password zdx1989 \
--table widgets2 \
--class-name WidgetHolder \
--jar-file WidgetHolder.jar \
--export-dir widget_sequence_files