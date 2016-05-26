import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object Article {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("COSC282 Filter")
    val sc = new SparkContext(sparkConf)
    val txt = sc.textFile("./wikidump.small.xml")

    val xml = txt.map{ l =>
      val parse = XML.loadString(l)
      val title = (parse \ "title").text
      val text = (parse \ "revision" \ "text").text//.replaceAll("\\W", " ")
      val ns = (parse \ "ns").text
      (title, ns, text)
    }

    val articles = xml.filter { r => isArticle(r._3.toLowerCase) }

    articles.map(r => (r._1,  r._3)).saveAsTextFile("./small-articles", classOf[GzipCodec])
  }

  def isArticle(txt: String) : Boolean = {
    val stubRegex = """(stub}}|wikipedia:stub)""".r
    val redirectRegex = """#redirect""".r
    val disambRegex = """\{\{disambig\w*\}\}""".r

    if (stubRegex.findFirstIn(txt) == None &&
      redirectRegex.findFirstIn(txt) == None &&
      disambRegex.findFirstIn(txt) == None) {
      true
    } else {
      false
    }
  }

}
