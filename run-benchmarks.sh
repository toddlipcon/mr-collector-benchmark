#!/bin/bash -e

COLLECTORS_TO_BENCHMARK='
  org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator
  org.apache.hadoop.mapred.MapTask$MapOutputBuffer
'

cd $(dirname $BASH_SOURCE)

if [ -z "$NATIVE_SO_PATH" ] || ! [ -f "$NATIVE_SO_PATH/libnativetask.so" ] ; then
  echo 'Please set $NATIVE_SO_PATH to the directory containing libnativetask.so'
  exit 1
fi

echo First, compiling benchmark code...
echo --------------------------------
mvn install

echo Generating classpath
echo --------------------------------
mvn dependency:build-classpath -Dmdep.outputFile=target/classpath.txt

mkdir -p results

echo --------------------------------
for i in $(seq 1 10) ; do
  for collector in $COLLECTORS_TO_BENCHMARK ; do
    sync
    echo Testing collector $collector
    java -Xmx1g \
        -Djava.library.path=$NATIVE_SO_PATH \
        -cp $(cat target/classpath.txt):target/classes \
        org.cloudera.mrbenchmark.MapOutputCollectorBenchmark \
        $collector | tee results/$collector.$i.txt
  done
done
