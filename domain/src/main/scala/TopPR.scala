import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast

object TopicPR {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("COSC282 Extract Link")
    val sc = new SparkContext(sparkConf)

    val football = sc.textFile("./football/part*").map(r => "[[" + r + "]]").toArray
    val gu = sc.textFile("./gu/part*").map(r => "[[" + r + "]]").toArray

    // page rank below
    val data = sc.textFile("./small-links-tab/*.gz")
    val  bTopicPages = sc.broadcast(football.toSet)
    val iLinks = data.map(_.split("\t", 2)).map(r => ("[["+r(0)+"]]", r(1).split("\t").toSet))
    calcPageRank(iLinks, 0.15, 10, "./small-tpg-tab-n10", bTopicPages)
  }


  def separateLinks(links: Array[String], topics: Array[String]): (Set[String], Set[String]) = {
    val topicLinks = links.toSet.intersect(topics.toSet)
    val otherLinks = links.toSet.diff(topics.toSet)


    return (topicLinks, otherLinks)

  }

  def calcPageRank(links: RDD[(String, Set[String])], weight: Double, niters: Int, path: String, bTopicPages: Broadcast[Set[String]]) = {
    var pageRanks = links.map(r => (r._1, .0))
    val topicPages = bTopicPages.value

    // it takes hours to run with about 12 million pages
    for (i <- 0 until niters) {
      val contributions = links.join(pageRanks).values.flatMap {
        case (links, pageRank) =>
          links.map(dest => (dest, pageRank / links.size))
      }
      val pageRanks1 = contributions.reduceByKey((x, y) => x + y).filter( r => topicPages.contains(r._1)).mapValues(v => weight + (1 - weight)*v)
      val pageRanks2 = contributions.reduceByKey((x, y) => x + y).filter( r => !topicPages.contains(r._1)).mapValues(v => (1 - weight)*v)
      pageRanks = pageRanks1.union(pageRanks2)
    }
    pageRanks.sortBy(_._2, false).map(r => r._1 + "\t" + r._2).saveAsTextFile(path)
  }

}
