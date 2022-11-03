hadoop fs -rmr /thread
hadoop fs -mkdir /thread
hadoop fs -mkdir /rohit
echo "Enter some value"
read max
a='rohit/temp'
for (( c=1; c<=$max; c++ ))
do
hadoop fs -mkdir  /$a$c

#echo "Welcome $c times..."
done
hadoop fs -ls /thread
