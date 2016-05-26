##Description

This is a project implementing Google's PageRank algorithm on **_Spark_**, programming in **_Scala_**. We impplement the approach using Wikipedia recent dump ([Wiki2015dump](https://dumps.wikimedia.org/enwiki/20150901/)). The dataset is about 50 GB. 

The project contains four subprojects: 

1. **Preprocess**: We first extract all the pages from the dump and conduct some text preprocessing.
2. **Filter**:We filter stubs, redirects and disambiguation pages.
3. **PageRank**: We implement the PageRank algorithm in this subproject.
4. **Domain-Specific PageRank**: We implement domain specific pagerank algorithm for two domains: Football, and Georgetown University, respectively. 



##Scratch of steps.

#### 1.Download wiki
Wiki dump at http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2 is about 50G uncompressed.

Run following command on a spark cluster, it will save the wiki dump to HDFS.

> curl -s -L http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2 | bzip2 -cd | hadoop fs -put - /mnt/wangs/wikidump.xml

#### 2. Download libraries

###### 2.1 text book git hub repo
You need mvn to compile the packge, and you will need common-1.0.1.jar under target directory.

> git clone https://github.com/sryza/aas.git
> cd aas
> mvn package

###### 2.2 cloud9 git hub repo
You need mvn to compile the package and you will need cloud9-2.0.2-SNAPSHOT-fatjar.jar under target directory.
 
> git clone git+ssh://git@github.com/lintool/Cloud9.git
> cd Cloud9
> mvn package

###### 3.a start spark shell on spark cluster master node, you can copy the *.shell command into shell to run it
> spark/bin/spark-shell --jars /mnt/wangs/common-1.0.1.jar,/mnt/wangs/cloud9-2.0.2-SNAPSHOT-fatjar.jar

###### 3.b alternatively, you can compile the program into a jar and run it on the cluster.
###### 3.b.1 compile on machine with sbt installed
> sbt assembly
###### 3.b.2 run the script on the cluster, it takes hours to run every thing
> sh /path/to/run.sh

#### 4. once standard page rank and topic page rank results are saved, we can query on spark shell
> spark-shell
check Query.scala.shell for commands



##Pratical Issues

#### 1. Size of data
There are more than 15 million pages in the wiki dump, and more than 3 millions are actual article pages. it is more efficient to select articles only for the project.

#### 2. Data filtering
If we only select articles, it is better only include only links among articles only

#### 3. Links
In wiki dump, links are surrounded by [[ and ]]. each article can refer to the same link multiple times, we need to consider one occurrence during page rank calculation

#### 4. PageRank
Given the size of the data, it takes some time for PageRank algorithm to converge.

#### 5. Broadcast variable (shared variable) in spark
In general it is not possible to pass a large broadcast variable around in the cluster. an variable with about 10,000 elements are about the maximum size. when you see java.heap out of memory error, it means broadcast variable is too big. consider using alternatives instead of broadcast variable in such case.

#### 6. Noise in data
Wiki dump is a relative clean data, but there are still small portions of unexpected noise. filter function is handy to ignore these noise and you can see examples in the code. 