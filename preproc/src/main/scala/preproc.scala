import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable.StringBuilder

object PreProc {
  def main(args: Array[String]) {
    // input file
    val filename = "./wikidump.small"
    // output file
    val writer = new PrintWriter(new File("./wiki-small.preproc"))
    var nPage = 0
    var nLine = 0
    var page = new StringBuilder

    // iterate through all lines
    for (line <- Source.fromFile(filename).getLines) {
      val trimmedLine = line.trim

      if (isStart(trimmedLine)) { // start of page
        page.append(trimmedLine)
      } else if (isEnd(trimmedLine)) { // end of page
        writer.write(page + trimmedLine + "\n")
        page.clear
        nPage += 1
      } else if (page.length > 0) { // concatenate the rest
        page.append(trimmedLine)
      }
      nLine += 1
      // print status of the process
      if (nLine  % 1000000 == 0) {
        println(nLine)
      }
    }
    writer.close
    println(nPage)
  }

  def isStart(line: String) : Boolean = {
    line.equals("<page>")
  }

  def isEnd(line: String) : Boolean = {
    line.equals("</page>")
  }
}
