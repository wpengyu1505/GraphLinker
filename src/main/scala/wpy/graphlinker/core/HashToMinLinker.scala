package wpy.graphlinker.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import wpy.graphlinker.model.NodeParser
import org.apache.spark.rdd.RDD
import wpy.graphlinker.model.Cluster
import org.apache.spark.Accumulator
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

class HashToMinLinker extends Serializable { 
  var delimiter = ','
  
  def link(inputFile: String, output: String) {
    val conf = new SparkConf().setAppName("Hash-To-Min Graph Linker").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val inputClusters = sc.textFile(inputFile).map(v => (new NodeParser()).getCluster(v))
    
    var graphFinal = resolveGraph(sc, inputClusters, 10)
    graphFinal.map(_.toString).saveAsTextFile(output)
  }
  
  def resolveGraph(sc: SparkContext, clusters: RDD[Cluster], iterations: Int): RDD[(Cluster)] = {
    
    var iterateCnt = 0
    var outClusters = clusters
    var changeCount = 1L
    while (iterateCnt < iterations && changeCount > 0) {
      var changeCnt = sc.accumulator(0L)
      var result = iterate(outClusters, changeCnt)
      outClusters = result
      iterateCnt += 1
      changeCount = changeCnt.value
      
      println("Iteration: " + iterateCnt + ": " + changeCount)
    }
    outClusters
  }
  
  def iterate(cluster: RDD[Cluster], changeCnt: Accumulator[Long]) : RDD[Cluster] = {
    val out = cluster.flatMap(v => {
      val buffer = new ListBuffer[(String, Cluster)]
      
      val id = v.getId
      val nodes = v.getNodeSet
      if (nodes.size > 1) {
        val lowest = v.getLowestNodeId()
        nodes.foreach(x => {
          if (x == lowest) {
            buffer.+=((x, new Cluster(x, nodes)))
          } else {
            val set = new HashSet[String]
            set.+=(lowest)
            buffer.+=((x, new Cluster(x, set)))
          }
        })
      }
      buffer.toList
    }).reduceByKey((a, b) => {
      var c = mergeClusters(a, b)
      if (!c.equals(a) || !c.equals(b)) {
        changeCnt += 1
      }
      c
    }).map(_._2)
    out.count
    out
  }
  
  def mergeClusters(c1: Cluster, c2: Cluster): Cluster = {
    val outCluster = c1;
    if (c1.getId() == c2.getId()) {
      c2.getNodeSet().foreach(v => {
        c1.merge(v)
      })
    }
    c1
  }
}