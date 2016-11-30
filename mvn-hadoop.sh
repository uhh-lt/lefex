#!/bin/bash
MAIN_CLASS=$1
shift # remove first argument (name of main class) from $@
# Compile list of jars of dependencies that will be added to Hadoop's classpath ...
mvn -DskipTests compile package dependency:build-classpath -Dmdep.outputFile=".dependency-jars" &&
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`cat .dependency-jars` &&
# ... and add jars of project (instead of specifying -jar argument to Hadoop)
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`echo target/*.jar | tr " " ":"` &&
LIBJARS=`echo $HADOOP_CLASSPATH | sed s/:/,/g` &&
HADOOP_CLASSPATH=$HADOOP_CLASSPATH hadoop $MAIN_CLASS -libjars $LIBJARS $@
