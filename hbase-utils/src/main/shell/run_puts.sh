#!/usr/bin/env bash

base_dir=$(cd `dirname $0`/..; pwd)
export CONF_DIR=$base_dir/conf



LOG4J=$base_dir/conf/log4j.properties
if [ -f $LOG4J ]; then
    export LOG4J_PARAMS="-Dlog4j.configuration=file:$LOG4J"
fi

jars=$base_dir/hbase-utils-1.0-SNAPSHOT.jar
for jar in ${base_dir}/lib/*jar
do
jars=$jars:$jar
done

java -cp $jars $LOG4J_PARAMS  hbase.utils.HbaseQuery $@
