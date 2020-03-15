package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * MapPartitons(func)  传递的函数在每个分区中执行一次 ,有可能出现OOM,
  *     内存空间较大时建议使用,提高效率
  */
object MapPartitonsDemo {
    def main(args: Array[String]): Unit = {
        val conf  = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
        val sc = new SparkContext(conf)
        val list = List(1,20,26,12,10)
        val rdd1 = sc.parallelize(list,4)
        //分区内的数据进行操作
//        val rdd2 = rdd1.mapPartitions(x => x.map(_*2))
        //会边加载边转换,转换完之后清除缓存
        val rdd2 = rdd1.mapPartitions(x =>{
            println("qqq")
           x.toList    //将分区中的数据全部加载在内存,可能导致oom
            x.map(_ * 2)
        })

        rdd2.collect.foreach(println)
        sc.stop()
    }
}
