name := "spark-centrality"

version := "0.11"

scalaVersion := "2.10.4"

organization := "cc.p2k"

libraryDependencies += "com.twitter" % "algebird-core_2.10" % "0.10.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

sparkComponents += "graphx"

spName := "webgeist/spark-centrality"

sparkVersion := "1.4.0"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")