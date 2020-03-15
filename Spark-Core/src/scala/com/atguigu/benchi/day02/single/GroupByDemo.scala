package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * GroupBy算子:分组后同一组的数据在同一分区
  */
object GroupByDemo {
    def main(args: Array[String]): Unit = {
        //1.创建SparkContext对象.建立连接
        val conf = new SparkConf().setMaster("local[2]").setAppName("groupBy")
        //
        val sc = new SparkContext(conf)
        //2.获取RDD数据集
        val list = List(20,1,30,43,50,28)
        //
        val rdd = sc.parallelize(list,2)
        val rdd1 = rdd.groupBy(x => x % 2)
        //求两个分区各自的和
        val rdd2 = rdd1.map{
            case (k,it) => (k,it.sum)
        }
        rdd2.collect.foreach(println)
        sc.stop()


    }
}
