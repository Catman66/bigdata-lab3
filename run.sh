echo an input dir must be prepared, with input data files inside
echo hdfs and yarn must be initiated, use init.sh to start them


input=input
trianglecounter=/home/catman/bigdata/lab3/trianglecounter

hdfs dfs -rm -r $input
hdfs dfs -put $input
hdfs dfs -rm -r output

cd $trianglecounter
# mvn package 
# mvn jar:jar

hadoop jar $trianglecounter/target/trianglecounter-2.0-SNAPSHOT.jar cn.catman.TriangleCounter $input output
cd ..

rm -r output
hdfs dfs -get output

