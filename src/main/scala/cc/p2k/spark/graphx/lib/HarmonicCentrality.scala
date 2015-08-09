package cc.p2k.spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._
import collection.immutable
import scala.language.postfixOps
import com.twitter.algebird._


object HarmonicCentrality extends Logging {

  /** mapping neighbours */
  type NMap = immutable.Map[Int, HLL]

  val BIT_SIZE = 12

  val hll = new HyperLogLogMonoid(BIT_SIZE)

  /**
   * Harmonic Centrality for node
   * @param vertexId VertexId
   * @param graph Graph
   * @return
   */
  def personalizedHarmonicCentrality[VD, ED](vertexId: VertexId, graph: Graph[VD, ED]): Double = {
    val initialGraph: Graph[Double, Int] = graph.mapVertices(
      (id, _) => {
        if (id == vertexId) 0.0
        else Double.PositiveInfinity
      }
    ).mapEdges(_ => 1)

    val vp = (id: VertexId, dist: Double, newDist: Double) => math.min(dist, newDist)

    def mergeMsg(a: Double, b: Double) = math.min(a, b)

    def sendMsg(triplet: EdgeTriplet[Double, Int]) = {
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }

    val paths = initialGraph.pregel(Double.PositiveInfinity)(vp, sendMsg, mergeMsg)

    val hr = paths.vertices.map[Double](
      tuple => {
        if (tuple._1 == vertexId) 0
        else 1 / tuple._2
      }
    ).reduce((a, b) => a + b)

    hr
  }

  /**
   * Harmonic Centrality for all nodes
   * @param graph Graph
   * @param maxDistance Int
   * @return
   */
  def harmonicCentrality[VD, ED](graph: Graph[VD, ED], maxDistance: Int = 6): Graph[Double, Int] = {

    val initGraph: Graph[NMap, Int] = graph.mapVertices(
      (id: VertexId, v: VD) => immutable.Map[Int, HLL](
        (0, hll.create(id.toString.getBytes))
      )
    ).mapEdges(_ => 1)

    val initMessage = immutable.Map[Int, HLL](0 -> new HyperLogLogMonoid(BIT_SIZE).zero)

    def incrementNMap(p: NMap): NMap = p.filterKeys(_ < maxDistance).map {
      case (v, d) => (v + 1) -> d
    }

    /**
     * program which runs on each vertex
     * @param x VertexId
     * @param vertexValue Double VD
     * @param message Double message type
     * @return VD
     */
    def vertexProgram(x: VertexId, vertexValue: NMap, message: NMap) = {
      addMaps(vertexValue, message)
    }

    /**
     * function that is applied to out
     * edges of vertices that received messages
     * @param edge EdgeTriplet  EdgeTriplet[VD, ED]
     * @return Iterator[(VertexId, A)]
     */
    def sendMessage(edge: EdgeTriplet[NMap, Int]): Iterator[(VertexId, NMap)] = {
      val newAttr = incrementNMap(edge.srcAttr)

      if (!isEqual(edge.dstAttr, newAttr)) {
        Iterator((edge.dstId, newAttr))
      } else {
        Iterator.empty
      }
    }

    /**
     * merge messages
     * @param a NMap
     * @param b NMap
     * @return VD
     */
    def messageCombiner(a: NMap, b: NMap): NMap = {
      addMaps(a, b)
    }

    val distances = Pregel(initGraph, initMessage, activeDirection = EdgeDirection.In)(
      vertexProgram, sendMessage, messageCombiner)

    distances.mapVertices[Double]((id: VertexId, vertexValue: NMap) => calculateForNode(id, vertexValue))
  }

  private def addMaps(nmap1: NMap, nmap2: NMap): NMap = {
    (nmap1.keySet ++ nmap2.keySet).map({
      k => k -> (
        nmap1.getOrElse(k, new HyperLogLogMonoid(BIT_SIZE).zero) +
        nmap2.getOrElse(k, new HyperLogLogMonoid(BIT_SIZE).zero))
    }).toMap
  }

  /**
   * neighbours count does not change by b
   * @param a NMap
   * @param b NMap
   * @return boolean
   */
  private def isEqual(a: NMap, b: NMap): Boolean = {
    val newVal = addMaps(a, b)
    for (key <- b.keySet) {
      val oldSize = a.getOrElse(key, new HyperLogLogMonoid(BIT_SIZE).zero).estimatedSize
      val newSize = newVal.getOrElse(key, new HyperLogLogMonoid(BIT_SIZE).zero).estimatedSize
      if (oldSize != newSize) {
        return false
      }
    }
    true
  }

  /**
   *
   * @param id VertexId
   * @param distances NMap
   * @return Double
   */
  private def calculateForNode(id:VertexId, distances: NMap): Double = {
    var harmonic = 0.0
    val sorted = distances.filterKeys(_ > 0).toSeq.sortBy(_._1)
    var total = hll.create(id.toString.getBytes)
    for ((step, v) <- sorted) {
      val before = total.estimatedSize
      total += v
      val after = total.estimatedSize
      harmonic += BigDecimal((after - before) / step)
        .setScale(5, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
    }
    harmonic
  }

}
