#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Parse the corpus in the CoNLL format."
    echo "parameters: <corpus-directory> <output-directory> <compress-output>"
    exit
fi

bin_hadoop=`echo target/lib/*.jar target/lefex*.jar`  # binaries are in the default location (mvn package)
input=$1
output=$2
compress=$3
parser="malt"  # or "stanford"
collapsing="true"
inputType="document"

echo "Corpus: $input"
if  hadoop fs -test -e $input  ; then
    echo "Corpus exists: true"
else
    echo "Corpus exists: false"
fi
echo ""

echo "To start press any key, to stop press Ctrl+C"
read -n 2


jars=`$bin_hadoop | tr " " ","`
path=`$bin_hadoop | tr " " ":"`
HADOOP_CLASSPATH=$path hadoop \
    de.uhh.lt.lefex.CoNLL.HadoopMain \
    -libjars $jars \
    -Dmapreduce.reduce.failures.maxpercent=10 \
    -Dmapreduce.map.failures.maxpercent=10 \
    -Dmapreduce.job.queuename=default \
    -Dmapreduce.map.java.opts=-Xmx8096m \
    -Dmapreduce.map.memory.mb=8000 \
    -DparserName=$parser \
    -Dcollapsing=$collapsing \
    -DinputType=$inputType \
    $input \
    $output \
    $compress

