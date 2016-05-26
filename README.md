This is a project implementing Google's PageRank algorithm on **_Spark_**, programming in **_Scala_**. We impplement the approach using Wikipedia recent dump ([Wiki2015dump](https://dumps.wikimedia.org/enwiki/20150901/)). The dataset is about 50 GB. 

The project contains four subprojects: 

1. **Preprocess**: We first extract all the pages from the dump and conduct some text preprocessing.
2. **Filter**:We filter stubs, redirects and disambiguation pages.
3. **PageRank**: We implement the PageRank algorithm in this subproject.
4. **Domain-Specific PageRank**: We implement domain specific pagerank algorithm for two domains: Football, and Georgetown University, respectively. 
