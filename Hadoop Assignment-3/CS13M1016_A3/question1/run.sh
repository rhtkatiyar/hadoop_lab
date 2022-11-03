clear
javac *.java 
javac main.java 
jar -cvf 1.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output1
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input1
hadoop fs -copyFromLocal input12 /
clear
hadoop jar 1.jar main /input12 /output1
hadoop fs -ls /output1
hadoop fs -cat /output1/part-r-00000


