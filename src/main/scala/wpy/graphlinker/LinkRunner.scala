package wpy.graphlinker

import wpy.graphlinker.core.HashToMinLinker

object LinkRunner {
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Must provide input and output path")
    }
    
    val input = args(0)
    val output = args(1)
    
    val linker = new HashToMinLinker()
    linker.link(input, output)
  }
}