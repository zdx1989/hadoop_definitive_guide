create 'test', 'data'

list

put 'test', 'row1', 'data:1', 'value1'
put 'test', 'row2', 'data:2', 'value2'
put 'test', 'row3', 'data:3', 'value3'

scan 'test'

disable 'test'
drop 'test'