 sqoop import \
 --connect jdbc:mysql://localhost/hadoopguide \
 --username root \
 --password zdx1989 \
 --table widgets \
 --class-name WidgetHolder \
 --as-sequencefile \
 --target-dir widget_sequence_files \
 --bindir .
