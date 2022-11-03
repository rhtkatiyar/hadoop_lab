clear
rm -r *.class
javac *.java 
javac main.java 
rm 6.jar
jar -cvf 6.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output6
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input62
hadoop fs -copyFromLocal input62 /
clear
hadoop jar 6.jar main /input62 /output6
hadoop fs -ls /output6
hadoop fs -cat /output6/part-r-00000


