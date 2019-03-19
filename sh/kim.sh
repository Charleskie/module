#!/usr/bin/bash
source ~/.bashrc
rm -r /data/home/hadoop/kim/code/metastore_db/
rm -r metastore_db
sh /data/home/hadoop/wj/startQuery.sh < /data/home/hadoop/kim/code/kim.scala
DATE_HADOOP=$(date -d yesterday +"%Y-%m-%d")
DATE_LOCAL=$(date -d yesterday +"%Y%m%d")
MONTH=$(date -d yesterday +"%Y%m")
hadoop fs -cat Kim/data/out/${DATE_HADOOP}/pa* > /data/home/hadoop/DoubleKing/${MONTH}/${DATE_LOCAL}/warning/warning${DATE_LOCAL}.txt
rm -r metastore_db