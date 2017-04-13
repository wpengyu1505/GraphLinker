package wpy.graphlinker.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import wpy.graphlinker.model.NodeParser
import org.apache.spark.rdd.RDD
import wpy.graphlinker.model.Cluster
import org.apache.spark.Accumulator
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

class BSPLinker extends Serializable {
  
  def link(inputFile: String, output: String) {
    val conf = new SparkConf().setAppName("BSP Graph Linker").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inputKv = sc.textFile(inputFile).map(v => (v.split(",")(0), v.split(",")(1))).distinct
    
    var graphFinal = resolveGraph(sc, inputKv, 10)
    graphFinal.map(_.toString).saveAsTextFile(output)
  }
  
  def resolveGraph(sc: SparkContext, kv: RDD[(String, String)], iterations: Int): RDD[(String, String)] = {
    
    var iterateCnt = 0
    var outKv = kv
    var changeCount = 1L
    while (iterateCnt < iterations && changeCount > 0) {
      var changeCnt = sc.accumulator(0L)
      var result = iterate(outKv, changeCnt)
      outKv = result
      iterateCnt += 1
      changeCount = changeCnt.value
      
      println("Iteration: " + iterateCnt + ": " + changeCount)
    }
    outKv
  }
  
  def iterate(kv: RDD[(String, String)], cnt: Accumulator[Long]) : RDD[(String, String)] = {
    var chainNodeRDD = kv.flatMap{case(k, v) => {
      if (!k.equals(v)) {
        List[(String, (String, String))]((k, (v, null)), (v, (null, k)))
      } else {
        List[(String, (String, String))]()
      }
    }}
    
    var updateNodeRDD = chainNodeRDD.groupByKey().flatMap{case(k, v) => {
      
      var buffer = ListBuffer[(String, String)]()
      var min = null: String
      var change = false
      var parents = new HashSet[String]
      var children = new HashSet[String]
      v.foreach(w => {
        if (w._1 != null) parents.add(w._1)
        if (w._2 != null) children.add(w._2)
      })
      
      if (!parents.isEmpty && !children.isEmpty) {
        change = true
      }
      
      // Iterate parent
      var prntItr = parents.iterator
      while (prntItr.hasNext) {
        var prnt = prntItr.next()
        if (min == null) {
          min = prnt
        } else {
          
          // More than 1 parent exist, still not done
          change = true
          if (min.compareTo(prnt) > 0) {
            min = prnt
          }
        }
        
      }
      
      // Iterate children
      var childItr = children.iterator
      while (childItr.hasNext) {
        var child = childItr.next
        if (min == null) {
          min = k
        } else if (min.compareTo(child) > 0) {
          min = child
          change = true
        }
      }
      
      // Compare key
      if (min == null) {
        min = k
      } else if (min.compareTo(k) > 0) {
        min = k
        change = true
      }
      
      if (!parents.isEmpty) {
        // Iterate parent out
        prntItr = parents.iterator
        while (prntItr.hasNext) {
          var prnt = prntItr.next
          if (!prnt.equals(min)) buffer.+=((prnt, min))
        }
        
        // Iterate child out
        childItr = children.iterator
        while (childItr.hasNext) {
          var child = childItr.next
          if (!child.equals(min)) buffer.+=((child, min))
        }
        
        // Self out
        if (!k.equals(min)) buffer.+=((k, min))
      }
      
      if (change) {
        cnt += 1
      }
      
      buffer.toList
    }}
          
    // Important here we force action because the accumulator needs an action to take effect
    updateNodeRDD.count
    updateNodeRDD
  }
}