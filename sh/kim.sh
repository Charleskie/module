#!/usr/bin/bash
source ~/.bashrc
rm -rf metastore_db/;
sh /data/home/hadoop/wj/startQuery.sh < /data/home/hadoop/kim/code/kim.scala;
DATE=$(date -d yesterday +"%Y-%m-%d");
hadoop fs -cat Kim/data/out/${DATE}/pa* > /data/home/hadoop/kim/data/${DATE}.txt;
rm -rf metastore_db;