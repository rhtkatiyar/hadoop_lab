clear
javac *.java 
javac main.java 
rm 8.jar
jar -cvf 8.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output8
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input4
hadoop fs -rm  -r /input8
hadoop fs -copyFromLocal input4 /
hadoop fs -copyFromLocal input8 /

clear
hadoop jar 8.jar main /input4 /input8 /output8
hadoop fs -ls /output8
hadoop fs -cat /output8/part-r-00000


