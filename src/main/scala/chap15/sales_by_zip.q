sqoop export \
--connect jdbc:mysql://localhost/hadoopguide \
--username root \
--password zdx1989 \
--table sales_by_zip \
--export-dir /user/hive/warehouse/zip_profits \
--input-fields-terminated-by '\0001'