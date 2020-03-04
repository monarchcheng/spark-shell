package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  *
  */
object App {

  def main(args: Array[String]): Unit = {

    println(fun())

  }


  def fun() = {
    val conf = new SparkConf()
    conf.setAppName("kyrieFirstSpark")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val transFile = sc.textFile("E:\\shopdata.txt")

    val transData = transFile.map(_.split("#"))

    var transByCust = transData.map(tran => (tran(2).toInt, tran))

    transByCust.keys.distinct().count()

    transByCust.countByKey().values.sum

    transByCust.countByKey().toSeq.maxBy(_._2)

    var complTrans = Array(Array("2015-03-30", "11:59PM", "53", "4", "1", "0.00"))

    transByCust.lookup(53).foreach(tran => println(tran.mkString(",")))

    transByCust.mapValues(tran => {

      if (tran(3).toInt == 25 && tran(4).toDouble > 1) {
        tran(5) = (tran(5).toDouble * 0.95).toString
      }
      tran

    })


    transByCust = transByCust.flatMapValues(tran => {

      if (tran(3).toInt == 81 && tran(4).toDouble >= 5) {
        val cloned = tran.clone()
        cloned(5) = "0.00"
        cloned(3) = "70"
        cloned(4) = "1"
        List(tran, cloned)
      } else {
        List(tran)
      }

    })


    val amounts = transByCust.mapValues(t=>t(5).toDouble)
    val totals = amounts.foldByKey(0)(_+_).collect()
    totals.toSeq.maxBy(_._2)


    complTrans = complTrans:+Array("2015-03-30", "11:59PM", "76", "63", "1", "0.00")

    transByCust=transByCust.union(sc.parallelize(complTrans).map(t=>(t(2).toInt,t)))

    transByCust.map(t=>t._2.mkString("#"))

    val prods = transByCust.aggregateByKey(List[String]())(

      (prod,tran)=>prod:::List(tran(3)),
      _:::_
    )

    prods.collect()


  }
}
