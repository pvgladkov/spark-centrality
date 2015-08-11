package cc.p2k.spark.graphx

import cc.p2k.spark.graphx.lib.{Diameter, HarmonicCentrality}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class HarmonicCentralityTest extends FunSuite  with BeforeAndAfterAll{

  val conf = new SparkConf().setAppName("Spark").setMaster("local")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  override def afterAll(): Unit ={
    this.sc.stop()
  }

  test("all_1") {

    val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
      (1L, 1.0), (2L, 2.0), (3L, 3.0), (4L, 4.0)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1)
    ))

    val graph = Graph(vertices, edges)
    val center = HarmonicCentrality

    val valid = Map(1->1.5, 2->2.0, 3->1.5, 4->0)
    val hc = center.harmonicCentrality(graph)

    for ((k,v) <- hc.vertices.collect().iterator){
      assert(
        BigDecimal(v).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble == valid(k.toInt)
      )
    }
  }

  test("all_2") {

    val vertices: RDD[(Long, Int)] = sc.parallelize(Array(
      (1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 4)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1), Edge(2L, 4L, 1), Edge(5L, 1L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1), Edge(4L, 3L, 1), Edge(4L, 2L, 1), Edge(1L, 5L, 1)
    ))

    val graph = Graph(vertices, edges)
    val center = HarmonicCentrality

    val valid = Map(1->3.0, 2->3.5, 3->2.8, 4->2.8, 5->2.2)
    val hc = center.harmonicCentrality(graph)

    for ((k,v) <- hc.vertices.collect().iterator){
      assert(
        BigDecimal(v).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble == valid(k.toInt)
      )
    }
  }

  test("all_3") {

    val vertices: RDD[(Long, Int)] = sc.parallelize(Array(
      (1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 4)
    ))

    val edges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(1L, 2L, "1"), Edge(2L, 3L, "1"), Edge(3L, 4L, "1"), Edge(2L, 4L, "1"), Edge(5L, 1L, "1"),
      Edge(2L, 1L, "1"), Edge(3L, 2L, "1"), Edge(4L, 3L, "1"), Edge(4L, 2L, "1"), Edge(1L, 5L, "1")
    ))

    val graph = Graph(vertices, edges)
    val center = HarmonicCentrality

    val valid = Map(1->3.0, 2->3.5, 3->2.8, 4->2.8, 5->2.2)
    val hc = center.harmonicCentrality(graph)

    for ((k,v) <- hc.vertices.collect().iterator){
      assert(
        BigDecimal(v).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble == valid(k.toInt)
      )
    }
  }

  test("person_1"){

    val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
      (1L, 1.0), (2L, 2.0), (3L, 3.0), (4L, 4.0), (5L, 4.0)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1), Edge(2L, 4L, 1), Edge(5L, 1L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1), Edge(4L, 3L, 1), Edge(4L, 2L, 1), Edge(1L, 5L, 1)
    ))

    val graph = Graph(vertices, edges)
    val center = HarmonicCentrality

    val valid = Map(1->3.0, 2->3.5, 3->2.8, 4->2.8, 5->2.2)

    for ((k,v) <- valid){
      val value = center.personalizedHarmonicCentrality(k, graph)
      assert(
        BigDecimal(value).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble == valid(k.toInt)
      )
    }
  }

  test("person_2"){

    val vertices: RDD[(Long, Int)] = sc.parallelize(Array(
      (1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 4)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1), Edge(2L, 4L, 1), Edge(5L, 1L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1), Edge(4L, 3L, 1), Edge(4L, 2L, 1), Edge(1L, 5L, 1)
    ))

    val graph = Graph(vertices, edges)
    val center = HarmonicCentrality

    val valid = Map(1->3.0, 2->3.5, 3->2.8, 4->2.8, 5->2.2)

    for ((k,v) <- valid){
      val value = center.personalizedHarmonicCentrality(k, graph)
      assert(
        BigDecimal(value).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble == valid(k.toInt)
      )
    }
  }

}
