package wpy.graphlinker.model

import scala.collection.mutable.HashSet

class NodeParser {
  
  def getCluster(input: String): Cluster = {
    
    var fields = input.split(",")
    var nodeSet = new HashSet[String]
    var elect = null: String
    fields.foreach { x => {
      //var node = new Node(x)
      var node = x
      if (elect == null || elect > node) {
        elect = node
      }
      nodeSet.+=(node)
    }}
    new Cluster(elect, nodeSet)
  }
}