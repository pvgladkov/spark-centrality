package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import scala.language.postfixOps

import org.apache.spark.Logging
import org.apache.spark.graphx._


object HarmonicCentrality extends Logging {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxDistance: Int): Graph[Double, Double] = {

  }

}
