package wpy.graphlinker

import wpy.graphlinker.core.HashToMinLinker
import wpy.graphlinker.core.BSPLinker

object LinkRunner {
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Must provide input and output path")
    }
    
    val input = args(0)
    val output = args(1)
    
    val linker = new BSPLinker()
    linker.link(input, output)
  }
}