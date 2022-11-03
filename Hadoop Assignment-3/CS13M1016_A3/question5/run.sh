clear
javac *.java 
javac main.java 
rm 5.jar
jar -cvf 5.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output5
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input52
hadoop fs -copyFromLocal input52 /
clear
hadoop jar 5.jar main /input52 /output5
hadoop fs -ls /output5
hadoop fs -cat /output5/part-r-00000


