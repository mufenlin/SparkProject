package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/*
*Repartition算子,增加分区,一定会shuffle
 */
object Repartition {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Repartition")
        val sc =new SparkContext(conf)
        val list1 = List(30, 50, 73, 66, 19,80)
        val rdd1 = sc.parallelize(list1,5)
        println(rdd1.getNumPartitions)
        val rdd2 = rdd1.repartition(8)
        println(rdd2.getNumPartitions)
    }
}
