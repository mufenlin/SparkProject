package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Filter算子:过滤功能
  */
object FileterDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Filter")
        val sc =new SparkContext(conf)
        val list1 = List(30, 50, 73, 60, 10, 27)
        val rdd = sc.parallelize(list1,2)
        val rdd2 = rdd.filter(x => x%2 == 0)
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
