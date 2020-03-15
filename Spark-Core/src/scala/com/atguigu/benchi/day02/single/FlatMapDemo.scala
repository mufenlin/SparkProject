package com.atguigu.benchi.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * FlatMap(func) 算子,一进多出,func需要返回一个序列
  */
object FlatMapDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("FlatMap")
        val sc = new SparkContext(conf)
/*        val list = List(1 to 3, 1 to 5, 10 to 20)
        val rdd1 = sc.parallelize(list,2)
        val rdd2 = rdd1.flatMap(x => x)*/

        val list = List(1,2,3,4,5,6)
        val rdd1 =  sc.parallelize(list,2)
        //rdd2中存储这些元素,元素的平方,及元素的立方
//        val rdd2 =rdd1.flatMap(x => List(x,x*x,x*x*x))

        val rdd2 = rdd1.flatMap(x => if (x%2 ==0) List(x,x*x,x*x*x) else List[Int]())
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
