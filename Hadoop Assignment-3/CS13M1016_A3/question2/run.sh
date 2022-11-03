clear
javac *.java 
javac main.java 
rm 2.jar
jar -cvf 2.jar  *.class
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /output2
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input22
hadoop fs -copyFromLocal input22 /
clear
hadoop jar 2.jar main /input22 /output2 1 5
hadoop fs -ls /output2
hadoop fs -cat /output2/part-r-00000


