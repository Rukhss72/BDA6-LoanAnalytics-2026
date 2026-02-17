#!/usr/bin/env bash
set -e
source /etc/profile || true
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
jps
