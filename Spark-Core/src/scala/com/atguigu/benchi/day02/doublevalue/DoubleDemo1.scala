package com.atguigu.benchi.day02.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

object DoubleDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("DoubleValue")
        val sc =new SparkContext(conf)
        val list1 = List(30, 50, 73, 66, 19,80)
        val list2 = List(100, 50, 60, 58, 20,80)
        val rdd1 = sc.parallelize(list1,4)
        val rdd2 = sc.parallelize(list2,4)
        //RDD并集
//        val rddRes = rdd1.union(rdd2)

        //RDD交集
//        val rddRes = rdd1.intersection(rdd2)

        //RDD差集
//        val rddRes = rdd1.subtract(rdd2)

        //笛卡尔集
        val rddRes = rdd1.cartesian(rdd2)

        rddRes.collect.foreach(println)



        sc.stop()

    }
}
