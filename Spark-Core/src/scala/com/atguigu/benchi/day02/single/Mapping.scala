package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Map算子,返回一个新的RDD,进一出一,每次处理一个数据
  *def map[U: ClassTag](f: T => U): RDD[U] ={}
  */

object Mapping {
    def main(args: Array[String]): Unit = {
        //1.创建SparkContext对象.建立连接
        val conf = new SparkConf().setMaster("local[2]").setAppName("Mapping")
        //
        val sc = new SparkContext(conf)
        //2.获取RDD数据集
        val list = List(20,1,30,43,50,28)
        //
        val rdd = sc.parallelize(list)
        //3.RDD转换
//        val rdd2 = rdd.map(x => x *2)
        val rdd2 = rdd.map(_ * 2)
        //4.执行行动算子,收集RDD执行结果
        rdd2.collect().foreach(println)
        //5.关闭sc
        sc.stop()


    }
}
