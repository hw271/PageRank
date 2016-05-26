package cosc282;
import org.apache.spark.{SparkConf, Logging, SparkContext}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.io._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import collection.mutable.HashMap
import collection.immutable.Map
import collection.immutable.Set

import Array._
import java.lang.NumberFormatException
import scala.util.control.Breaks._

import com.typesafe.config._
import org.joda.time.DateTime
import com.cloudera.datascience.common.XmlInputFormat
import edu.umd.cloud9.collection.wikipedia.language._
import edu.umd.cloud9.collection.wikipedia._
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link

object PageRank {
  def main(args: Array[String]) {
    //var conf = ConfigFactory.load
//    @transient val hadoopConf = new Configuration()
//    hadoopConf.set(XmlInputFormat.START_TAG_KEY, "<page>")
//    hadoopConf.set(XmlInputFormat.END_TAG_KEY, "</page>")

    val sparkConf = new SparkConf().setAppName("COSC282 Homework")
    val sc = new SparkContext(sparkConf)
    
    val wikiDumpPath = "/Users/hw271/Documents/svn/cosc282/assignments/project_shuguang/preproc/wikidump.small"
    // 0. load raw wiki xml files and select only articles
    //val wikiDumpPath = conf.getString("hdfs.wiki.path")
//    val kvs = sc.newAPIHadoopFile(wikiDumpPath, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
//    val rawXmls = kvs.map(p => p._2.toString)
//    val articles = rawXmls.filter( r => isArticle(r) )
    //val niters = conf.getInt("pagerank.num.iterations")

    // // 1.a extract links from scratch, it requires step 0
    // val links = articles.flatMap(xml=>wikiXmlToLinks) ele1:title ele2: list of links 
    // val nonEmptyLinks = links.filter( r => (r != null && r._1.trim != "" && r._2.size > 0) )//.map( r => trimLinks(r, bArticleIds) ).filter( r => r._2.size > 0 )
    // nonEmptyLinks.cache
    // nonEmptyLinks.map(r => r._1 + ":::" + r._2.mkString("---")).saveAsTextFile(conf.getString("hdfs.link.path"))

    // 1.b offline, remove non-articles from links, this can be optional but affect the rank
    // # extract links files into a single local file
    // > hadoop fs -text /path/to/links/part* > /local/path/to/links.txt
    // > python trimLinks.py -i /local/path/to/links.txt -o /local/path/to/trimlinks.txt
    // # load the local trimmed links back into hdfs
    // > hadoop fs -put /local/path/to/trimlinks.txt /hdfs/path/to/trimlinks.txt

    // 1.c we can load the links from hdfs if it is available, this step and 1.a are exclusive to each other
//    val nonEmptyLinks = sc.textFile("hdfs:///mnt/wangs/wikidump.trimlinks").map(_.split(":::")).map(r => (r(0), r(1).split("---").toSet)).cache

    // 2. compute standard page rank
//    calcPageRank(nonEmptyLinks, 0.15, niters, conf.getString("hdfs.pagerank.path"), sc.broadcast(Set()))

    // // 3.a extract categories from scratch, it requires step 0
    // val categories = articles.flatMap(wikiXmlToCats)
    // val nonEmptyCategories = categories.filter( r => (r != null && r._1 != "" && r._2.size > 0) )
    // nonEmptyCategories.cache
    // nonEmptyCategories.map(r => r._1 + ":::" + r._2.mkString("---")).saveAsTextFile(conf.getString("hdfs.cat.path"))
    // nonEmptyCategories.map(r => r._2.mkString("\t")).flatMap(_.split("\t")).map(r => (r, 1)).reduceByKey(_+_).sortBy(_._2, false).map(r => r._1 + "\t" + r._2).coalesce(1).saveAsTextFile(conf.getString("hdfs.catstat.path"))

    // 3.b load categories from saved files, this step and 3.a are exclusive to each other
//    val nonEmptyCategories = sc.textFile("hdfs:///mnt/wangs/wikidump.categories/part*").map(_.split(":::")).map(r => (r(0), r(1).split("---"))).cache

    // 4. compute page rank given a topic
//    for (topic <- conf.getString("wiki.topic.list").split(",")) {
//      val bTopicPages = sc.broadcast(nonEmptyCategories.filter(r => r._2.contains(topic)).map(r => r._1).toArray.toSet)
//
//      calcPageRank(nonEmptyLinks, 0.15, niters, conf.getString("hdfs.pagerank.path") + "_" + topic.replace(" ", "_"), bTopicPages)
//    }

    sc.stop()
  }

  // not used
  def trimLinks(r: (String, Set[String]), bArticleIds: Broadcast[Set[String]]) : (String, Set[String]) = {
    var newLinks : Set[String] = Set()
    val articleIds = bArticleIds.value

    for(link <- r._2) {
      if (articleIds.contains(link)) {
        newLinks += link
      }
    }

    (r._1, newLinks)
  }

  def calcPageRank(links: RDD[(String, Set[String])], weight: Double, niters: Int, path: String, bTopicPages: Broadcast[Set[String]]) = {
    var pageRanks = links.mapValues(v => .0)
    val topicPages = bTopicPages.value

    // it takes hours to run with about 12 million pages
    for (i <- 0 until niters) {
      val contributions = links.join(pageRanks).values.flatMap {
        case (pageLinks, pageRank) =>
            pageLinks.map(dest => (dest, pageRank / pageLinks.size))
      }
      if (topicPages.size > 0) {
        val pageRanks1 = contributions.reduceByKey((x, y) => x + y).filter( r => topicPages.contains(r._1)).mapValues(v => weight + (1 - weight)*v)
        val pageRanks2 = contributions.reduceByKey((x, y) => x + y).filter( r => !topicPages.contains(r._1)).mapValues(v => (1 - weight)*v)
        pageRanks = pageRanks1.union(pageRanks2)
      } else {
        pageRanks = contributions.reduceByKey((x, y) => x + y).mapValues(v => weight + (1 - weight)*v)
      }
    }
    pageRanks.map(r => r._1 + ":::" + r._2).saveAsTextFile(path)
  }

  // not used
  def matchTopic(rank: Double, links: Set[String], topicPages: Set[String]) : Double = {
    var count = 0
    for (link <- links) {
      if (topicPages.contains(link)) {
        count += 1
      }
    }
    if (count == 0) {
      0.
    } else {
      rank / count
    }
  }

  def isArticle(r: String) : Boolean = {
    val page = new EnglishWikipediaPage()
    WikipediaPage.readPage(page, r)
    page.isArticle && !page.isStub && !page.isEmpty && !page.isRedirect && !page.isDisambiguation
  }

  def extractLinks(page: String) : Set[String] = {
    var links: Set[String] = Set()
    var start = 0

    breakable {
      while (true) {
        start = page.indexOf("[[", start)

        if (start < 0) {
          break
        }

        var end = page.indexOf("]]", start)

        if (end < 0) {
          break
        }

        var text = page.substring(start + 2, end)

        // skip empty links
        if (text.length() == 0) {
          start = end + 1
        } else {

          // skip special links
          if (text.indexOf(":") != -1) {
            start = end + 1
          } else {

            // if there is anchor text, get only article title
            var a = text.indexOf("|")
            if (a != -1) {
              text = text.substring(0, a)
            }

            a = text.indexOf("#")
            if (a != -1) {
              text = text.substring(0, a)
            }

            // ignore article-internal links, e.g., [[#section|here]]
            if (text.length() == 0) {
              start = end + 1
            } else {
              links  += text.replaceAll("\n", "").replaceAll("\r", "")
              start = end + 1
            }
          }
        }
      }
    }
    links
  }

  def wikiXmlToLinks(wikiXml: String) : Option[(String, Set[String])] = {
    val page = new EnglishWikipediaPage()
    WikipediaPage.readPage(page, wikiXml)
    if (page.isEmpty) Some(("", Set()))
    else Some((page.getTitle, extractLinks(wikiXml)))
  }


  def extractCategories(page: String) : Set[String] = {
    var categories: Set[String] = Set()
    var start = 0

    breakable {
      while (true) {
        start = page.indexOf("[[Category:", start)

        if (start < 0) {
          break
        }

        var end = page.indexOf("]]", start)

        if (end < 0) {
          break
        }

        var text = page.substring(start + 11, end)

        // skip empty categories                                                                                                                                                                        
        if (text.length() == 0) {
          start = end + 1
        } else {
          // extract categories                                                                                                                                                          
          val a = text.indexOf("|")
          if (a > 0) {
            text = text.substring(0, a)
          }
          categories += text.replaceAll("\n", "").replaceAll("\r", "")
          start = end + 1
        }
      }
    }
    categories
  }

  def wikiXmlToCats(wikiXml: String) : Option[(String, Set[String])] = {
    val page = new EnglishWikipediaPage()
    WikipediaPage.readPage(page, wikiXml)
    if (page.isEmpty) Some(("", Set()))
    else Some((page.getTitle, extractCategories(wikiXml)))
  }


}

