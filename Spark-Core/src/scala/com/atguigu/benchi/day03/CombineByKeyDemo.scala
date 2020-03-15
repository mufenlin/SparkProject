package com.atguigu.benchi.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * CombineByKey算子:和aggregate类似,只不过将零值转换为参数
  *
  * def combineByKey[C](
  * createCombiner: V => C,
  * mergeValue: (C, V) => C,
  * mergeCombiners: (C, C) => C): RDD[(K, C)] ={}
  */
object CombineByKeyDemo {
    def main(args:Array[String])={
        val  conf = new SparkConf().setMaster("local[2]").setAppName("PartitionBy")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
        val rdd= sc.parallelize(list1,2)
        //分区内和分区间每个key的和
/*        val rdd1 = rdd.combineByKey(
            v => v,
            (c:Int,v:Int) => c + v,
            (c1:Int,c2:Int) => c1 +c2
        )*/

        //每个key在每个分区里面的最大值,分区之间求出最大值的和
        val rdd1 = rdd.combineByKey(
            v=> v,
            (max:Int,v:Int) => max.max(v),
            (c1:Int,c2:Int) =>c1 +c2
        )


        rdd1.collect.foreach(println)
        sc.stop()
    }

}
