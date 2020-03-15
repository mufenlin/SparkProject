package com.atguigu.benchi.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ReduceByKey聚合算子,
  *  1)和scala( reduce,foldleft)不一样,scala最终都是聚合为一个值;
  *  2)spark的这个聚合,根据key来聚合的,结果是key的种类,操作的都是value
  *   在一个(K,V)的RDD上调用,返回一个新的(K,V) RDD
  *
  *  算子源码:
  *  def reduceByKey(func: (V, V) => V): RDD[(K, V)] ={}
  *
  */
object ReduceByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceBykey")
        val sc = new SparkContext(conf)
        val array = Array("hello", "hello", "world", "hello", "world", "hello", "world", "world")
        val rdd  = sc.parallelize(array,2)
        val rdd1 = rdd.map((_,1))

        val rdd2 = rdd1.reduceByKey(_ + _)
        rdd.collect.foreach(println)
        sc.stop()


    }
}
