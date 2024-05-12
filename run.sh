echo an input dir must be prepared, with input data files inside
echo hdfs and yarn must be initiated, use init.sh to start them


input=input
trianglecounter=$(pwd)/trianglecounter
rm -r output


hdfs dfs -rm -r $input
hdfs dfs -put $input
hdfs dfs -rm -r output

cd $trianglecounter
# mvn package 
# mvn jar:jar

hadoop jar $trianglecounter/target/trianglecounter-2.0-SNAPSHOT.jar cn.catman.TriangleCounter $input output
cd ..


hdfs dfs -get output
echo result: $(cat output/sum/part-r-00000) 
echo truth: $(cat truth)