clear
javac *.java 
javac main.java 
jar -cvf 1.jar  *.class
clear
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /rohit
clear
echo "Please Enter input file name : "
read input_variable
echo "Please Enter maximum memory size of system : "
read n;
hadoop fs -mkdir /rohit
hadoop fs -mkdir /rohit/temp0
hadoop fs -rm -r /output
hadoop fs -rm -r /temp
hadoop fs -rm -r /tmp
hadoop fs -copyFromLocal $input_variable /rohit/temp0
clear
hadoop jar 1.jar main $n
clear
hadoop fs -ls /rohit/output
