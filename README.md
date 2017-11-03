# Predictive Analytics Final Project 
This is the repository of predictive analytics' final project - Churn prediction - KKBOX music streaming service

### Intructions
1. cd to project directory
2. ~/sbt/bin/./sbt, type compile and then package
3. spark-submit --driver-memory 8G --executor-memory 8G --executor-cores 10 --driver-cores 10 --class "pa_final" /xxx/xxx/target/scala-2.10/pa-final_2.10-1.0.jar > log
