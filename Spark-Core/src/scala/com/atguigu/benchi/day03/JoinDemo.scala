package com.atguigu.benchi.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Join算子,只能适用于kv形式的rdd
  *
  */
object JoinDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Zip")
        val sc =new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        val rdd2 = sc.parallelize(Array((1, "aa"), (1, "dd"), (3, "bb"), (2, "cc")))
        //内连接
//        val rddRes = rdd1.join(rdd2)
        //左外连接
//        val rddRes = rdd1.leftOuterJoin(rdd2)
        //右外连接
//        val rddRes = rdd1.rightOuterJoin(rdd2)
        //全外连接
        val rddRes = rdd1.fullOuterJoin(rdd2)
        rddRes.collect.foreach(println)
        sc.stop()

    }
}
