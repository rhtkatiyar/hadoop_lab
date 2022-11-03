clear
javac *.java 
javac main.java 
jar -cvf gis.jar  *.class
clear
hadoop dfsadmin -safemode leave
hadoop fs -rm -r /rohit
clear

echo "Please Enter input data file name : "
read input_variable
echo "Please Enter input user file name : "
read user_input 
echo "Please Enter No of Class : "
read class
echo "Please Enter Alpa value : "
read alpa
echo "Please Enter K value : "
read k
echo "Please Enter No of users : "
read users

hadoop fs -mkdir /rohit
hadoop fs -mkdir /rohit/temp0
hadoop fs -mkdir /rohit/temp1
a='Thread'
b='0'
for (( c=0; c<$alpa; c++ ))
do
#hadoop fs -mkdir -p /$a$c$b
hadoop fs -mkdir /rohit/$a$c$b
hadoop fs -copyFromLocal $input_variable /rohit/$a$c$b
done
hadoop fs -copyFromLocal $user_input /rohit/temp1/
hadoop fs -rm -r /output
hadoop fs -rm -r /temp
hadoop fs -rm -r /tmp
hadoop fs -copyFromLocal $input_variable /rohit/temp0
clear
hadoop jar gis.jar main $class $alpa $k $users
#clear
#hadoop jar merge.jar Result $alpa
#hadoop fs -ls /rohit/
