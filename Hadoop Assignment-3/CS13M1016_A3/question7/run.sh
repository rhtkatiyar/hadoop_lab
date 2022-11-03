clear
rm -r *.class
javac *.java 
javac main.java 
rm 7.jar
jar -cvf 7.jar  *.class
hadoop dfsadmin -safemode leave
#hadoop fs -rm -r /input/input*

hadoop fs -rm -r /output7
hadoop fs -rm -r /tmp
hadoop fs -rm  -r /input7
#hadoop fs -copyFromLocal /home/hduser/assignment_3/question7/input /

#hadoop fs -copyFromLocal input7 /input
clear
hadoop jar 7.jar main 
#hadoop fs -ls /output7
#hadoop fs -cat /output7/part-r-00000


