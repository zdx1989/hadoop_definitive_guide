sqoop create-hive-table \
--connect jdbc:mysql://localhost/hadoopguide \
--username root \
--password zdx1989 \
--table widgets \
--fields-terminated-by ','

hive -e "load data inpath 'widgets' into table widgets"
