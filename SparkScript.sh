#!/bin/bash
#name=linux
#echo "Hello $name World!"
#echo ls -l some-non-existingfile.txt
#for i in $( ls ); do
# echo item: $i
#done
echo "--->> $(date '+%Y %b %d %H:%M:%S') :: Execution of JAR file begins"
command spark-submit --queue default  --class pradhan.com.spark.SparkSQLSample target/scala-2.11/SparkSampleSBT_2.11-2.1.1_1.0.jar 2>>spark-sbt-log.txt;
echo "--->> $(date '+%Y %b %d %H:%M:%S') :: Execution of JAR file ends"
echo
echo
echo "------------------------------------------------------------------------------------------------------------------------------"
echo "Assignment #1: Top 10 requested urls"
echo "------------------------------------------------------------------------------------------------------------------------------"
echo 
echo "URL  count"
echo "________________________________________________________"
command hdfs dfs -cat SparkProjectSQLsbt/Problem1/part-00000;
#echo
#echo "------------------------------------------------------------------------------------------------------------------------------"
#echo "Assignment #2: Top 5 timeframes for high traffice"
#echo "------------------------------------------------------------------------------------------------------------------------------"
#echo 
#echo "Host - count"
#echo "________________________________________________________"
#command hdfs dfs -cat SparkProjectSQLsbt/Problem2/part-00000;
echo
echo
echo "------------------------------------------------------------------------------------------------------------------------------"
echo "Assignment #2: Top 5 timeframes for high traffic"
echo "------------------------------------------------------------------------------------------------------------------------------"
echo 
echo "timeFrame  req_cnt"
echo "________________________________________________________"
command hdfs dfs -cat SparkProjectSQLsbt/Problem3/part-00000;
echo
echo
echo "------------------------------------------------------------------------------------------------------------------------------"
echo "Assignment #3: Top 5 timeframe for least traffic"
echo "------------------------------------------------------------------------------------------------------------------------------"
echo 
echo "timeframe  req_cnt"
echo "________________________________________________________"
command hdfs dfs -cat SparkProjectSQLsbt/Problem4/part-00000;
echo
echo
echo "------------------------------------------------------------------------------------------------------------------------------"
echo "Assignment #4: Unique HTTP codes returned by the server along with count "
echo "------------------------------------------------------------------------------------------------------------------------------"
echo 
echo "HTTP code  count"
echo "________________________________________________________"
command hdfs dfs -cat SparkProjectSQLsbt/Problem5/part-00000;
