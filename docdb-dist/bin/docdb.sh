#!/bin/sh

# Copyright Lealone Database Group.
# Licensed under the Server Side Public License, v 1.
# Initial Developer: zhh

if [ "x$DOCDB_HOME" = "x" ]; then
    export DOCDB_HOME="`dirname "$0"`/.."
fi

if [ "x$JAVA_HOME" = "x" ]; then
    echo JAVA_HOME environment variable must be set!
    exit 1;
fi

# JAVA_OPTS=-ea
# JAVA_OPTS="$JAVA_OPTS -Xms10M"
# JAVA_OPTS="$JAVA_OPTS -Xmx1G"
# JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
# JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
# JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
# JAVA_OPTS="$JAVA_OPTS -XX:+CMSParallelRemarkEnabled"
# JAVA_OPTS="$JAVA_OPTS -XX:SurvivorRatio=8"
# JAVA_OPTS="$JAVA_OPTS -XX:MaxTenuringThreshold=1"
# JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
# JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

JAVA_OPTS="$JAVA_OPTS"

if [ "$1" = "-debug" ]; then
    JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=y"
fi

CLASSPATH=$DOCDB_HOME/conf:$DOCDB_HOME/lib/*

"$JAVA_HOME/bin/java" $JAVA_OPTS -cp $CLASSPATH org.lealone.docdb.main.LealoneDocDB "$@"
