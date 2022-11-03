clear
javac *.java 
javac main.java 
jar -cvf mincut.jar  *.class
clear

hadoop dfsadmin -safemode leave
hadoop fs -rm -r /rohit
clear

echo "Please Enter input file name : "
read input_variable
echo "Please Enter maximum memory size of system : "
read eta
echo "Please Enter t value: "
read t
echo "Please Enter Alpa value : "
read alpa
echo "Please Enter Total No of vertex : "
read vertex_no

hadoop fs -mkdir /rohit
hadoop fs -mkdir /rohit/temp0

a='Thread'
b='0'
for (( c=0; c<$alpa; c++ ))
do
#hadoop fs -mkdir -p /$a$c$b
hadoop fs -mkdir /rohit/$a$c$b
hadoop fs -copyFromLocal $input_variable /rohit/$a$c$b
done

hadoop fs -rm -r /output
hadoop fs -rm -r /temp
hadoop fs -rm -r /tmp
hadoop fs -copyFromLocal $input_variable /rohit/temp0
clear

hadoop jar mincut.jar main $eta $t $alpa $vertex_no

clear
hadoop jar 1.jar Result $alpa

hadoop fs -ls /rohit/
