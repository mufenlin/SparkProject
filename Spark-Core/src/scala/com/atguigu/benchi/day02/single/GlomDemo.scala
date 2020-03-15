package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * glom算子: 将每一个分区中的元素合并为数组,
  */
object GlomDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Glom")
        //
        val sc = new SparkContext(conf)
        //2.获取RDD数据集
        val list = List(20,1,30,43,50,28)
        //numSlices为切片数
        val rdd = sc.parallelize(list,4)
        val rdd1 = rdd.glom().map(x => x.toList)
        rdd1.collect.foreach(println)
        sc.stop()
    }
}
