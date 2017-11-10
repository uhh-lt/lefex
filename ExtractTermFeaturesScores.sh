if [ -z "$1" ] || [ -z "$2" ] ; then
    echo "Extract word feature counts from the input text corpus"
    echo "parameters: <corpus-directory> <output-directory>"
    exit
fi

bin_hadoop=`echo target/lib/*.jar target/lefex*.jar`  # binaries are in the default location (mvn package)
hadoop_xmx_mb=8192
hadoop_mb=8000
queue=shortrunning
corpus=$1
output=$2

echo "Corpus: $corpus"
if  hadoop fs -test -e $corpus  ; then
    echo "Corpus exists: true"
else
    echo "Corpus exists: false"
fi
echo "Output: $output"
echo "To start press any key, to stop press Ctrl+C"
read -n 2

jars=`$bin_hadoop | tr " " ","`
path=`$bin_hadoop | tr " " ":"`
HADOOP_CLASSPATH=$path hadoop \
    de.uhh.lt.lefex.ExtractTermFeatureScores.HadoopMain \
    -libjars $jars \
    -Dmapreduce.reduce.failures.maxpercent=10 \
    -Dmapreduce.map.failures.maxpercent=10 \
    -Dmapreduce.job.queuename=$queue\
    -Dmapreduce.map.java.opts=-Xmx${hadoop_xmx_mb}m \
    -Dmapreduce.map.memory.mb=$hadoop_mb \
    -Dmapreduce.reduce.java.opts=-Xmx${hadoop_xmx_mb}m \
    -Dmapreduce.reduce.memory.mb=$hadoop_mb \
    -Dmapred.max.split.size=2000000 \
    -Dholing.mwe.vocabulary=src/main/resources/data/voc-ner.csv \
    -Dholing.mwe.ner=true \
    $corpus \
    $output \
    false
