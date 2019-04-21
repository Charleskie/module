DATE=$1;
spark-submit --master yarn-cluster --num-executors 100   --executor-memory 4G  --executor-cores 6  --driver-memory 4G --conf spark.default.parallelism=1000 --conf spark.storage.memoryFraction=0.3 --conf spark.shuffle.memoryFraction=0.5 --class cn.sibat.homeAndwork.MarkHomeWork ./spark/module.jar ${DATE} path_savefile path_busO path_subwayOD
#for i in {01..30};do sh MarkHomeWork.sh 201711$i;done
#执行for循环语句，输出文件为/HomeandWork/Homecal/{DATE} 和 /HomeandWork/Workcal/{DATE}
#注意修改spark程序参数，
#第一个参数为日期，调用for循环的日期，不用修改，
#第二个参数为保存路径，第三个为BusO的路径，第四个为SubwayOD的路径