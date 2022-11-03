clear
javac *.java 
javac main.java 
rm 3.jar
jar -cvf 3.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output3
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input32
hadoop fs -copyFromLocal input32 /
clear
hadoop jar 3.jar main /input32 /output3
hadoop fs -ls /output3
hadoop fs -cat /output3/part-r-00000


