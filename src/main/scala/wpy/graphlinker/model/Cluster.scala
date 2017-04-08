package wpy.graphlinker.model

import scala.collection.mutable.HashSet

class Cluster(var id: String, var nodeSet: HashSet[String]) {
  
  def merge(node: String) = {
    nodeSet.+=(node)
  }
  
  def getId(): String = {
    id
  }
  
  def getNodeSet(): HashSet[String] = {
    nodeSet
  }
  
  def getLowestNodeId(): String = {
    var low = null: String
    nodeSet.foreach(v => {
      if (low == null || low > v) {
        low = v
      }
    })
    low
  }
  
  override def toString: String = {
    var out = ""
    out += id + ":"
    nodeSet.foreach(v => out += v + ",")
    out.substring(0,out.length()-1)
  }
}