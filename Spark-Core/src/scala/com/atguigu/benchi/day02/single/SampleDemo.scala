package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Sample(withReplacement,fraction,seed)抽样算子,
  *    withReplacement是否放回抽样
  */
object SampleDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Sample")
        val sc = new SparkContext(conf)
        val list = 1 to 20
        val rdd1 =  sc.parallelize(list,2)
        //参数1,表示不放回抽样,比例[0~1]
        val rdd2 = rdd1.sample(false,0.2)
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
