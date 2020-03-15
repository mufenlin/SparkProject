package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * MapPartitionsWithIndex(func)  计算每个分区的索引,并会带上该分区的索引
  * def mapPartitionsWithIndex[U: ClassTag](
  * f: (Int, Iterator[T]) => Iterator[U],
  * preservesPartitioning: Boolean = false): RDD[U] = {}
  */

object MapPartitionsWithIndexDemo {
    def main(args: Array[String]): Unit = {
        val conf  = new SparkConf().setMaster("local[2]").setAppName("MapPartitionsWithIndex")
        val sc = new SparkContext(conf)
        val list = List(1,20,26,12,10)
        val rdd1 = sc.parallelize(list,4)
//        println(rdd1.getNumPartitions)
        val rdd2 = rdd1.mapPartitionsWithIndex((index,it)=>{
            it.map(x => (index,x))
        })

        rdd2.collect.foreach(println)
        sc.stop()

    }

}
