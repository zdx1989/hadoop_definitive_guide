sqoop import \
--connect jdbc:mysql://localhost/hadoopguide \
--username root \
--password zdx1989 \
--table widgets \
--hive-import -m 1
