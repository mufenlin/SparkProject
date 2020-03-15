package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Coalesce算子,合并分区
  *     默认不会有shuffle,(一个分区的数据进入多个分区时会有shuffle)
  *     不建议使用该方法增加分区.
  *
  */
object CoalesceDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Coalesce")
        val sc =new SparkContext(conf)
        val list1 = List(30, 50, 73, 66, 19,80)
        val rdd1 = sc.parallelize(list1,5)
        println(rdd1.getNumPartitions)

        /**
          * def coalesce(numPartitions: Int, shuffle: Boolean = false,
          * partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
          * (implicit ord: Ordering[T] = null)
          * : RDD[T]
          *
          * 减少分区时,默认不会有shuffle
          */
        val rdd2 = rdd1.coalesce(2)

        println(rdd2.getNumPartitions)

    }
}
