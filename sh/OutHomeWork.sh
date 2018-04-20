spark-submit --master yarn-cluster --num-executors 100   --executor-memory 6G  --executor-cores 5  --driver-memory 1G --conf spark.default.parallelism=1000 --conf spark.storage.memoryFraction=0.2 --conf spark.shuffle.memoryFraction=0.5 --class cn.sibat.homeAndwork.OutHomeWork ./spark/module.jar month date path_savefile

#spark-submit --master yarn-cluster --num-executors 100   --executor-memory 6G  --executor-cores 5  --driver-memory 1G --conf spark.default.parallelism=1000 --conf spark.storage.memoryFraction=0.2 --conf spark.shuffle.memoryFraction=0.5 --class cn.sibat.homeAndwork.OutHomeWork ./spark/module.jar 201711 201711* /user/wangsheng/
#输出文件为/HomeandWork/OutHome/month和/HomeandWork/OutWork/month
#直接运行也可，注意修改参数，第一个参数为月，第二个参数为当月所有日期，
#第三个为保存文件路径，保存文件路径要和MarkHomeWork的保存文件路径一致