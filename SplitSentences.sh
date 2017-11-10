#!/usr/bin/env bash
if [ -z "$1" ] || [ -z "$2" ] ; then
    echo "Split input corpus into one sentence per line, make the sentences unique, remove html."
    echo "parameters: <corpus-directory> <output-directory>"
    exit
fi


# Process input params
corpus=$1
output=$2
make_uniq=true
compress_output=false

jars=`$bin_hadoop | tr " " ","`
path=`$bin_hadoop | tr " " ":"`
HADOOP_CLASSPATH=$path hadoop \
    de.uhh.lt.lefex.SentenceSplitter.HadoopMain \
    -libjars $jars \
    -Dmapreduce.reduce.failures.maxpercent=10 \
    -Dmapreduce.map.failures.maxpercent=10 \
    -Dmapreduce.job.queuename=default \
    -Dmapreduce.map.java.opts=-Xmx4g \
    -Dmapreduce.map.memory.mp=4096 \
    -Dmapreduce.reduce.java.opts=-Xmx8g \
    -Dmapreduce.reduce.memory.mb=8192 \
    -Dtokenize=true \
    -Dmax_sentence_size=110 \
    -Dstrip_html=true \
    $corpus \
    $output \
    $make_uniq \
    $compress_output

