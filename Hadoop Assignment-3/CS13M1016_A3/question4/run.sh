clear
javac *.java 
javac main.java 
rm 4.jar
jar -cvf 4.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output4
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input42
hadoop fs -copyFromLocal input42 /
clear
hadoop jar 4.jar main /input42 /output4 1 5
hadoop fs -ls /output4
hadoop fs -cat /output4/part-r-00000


