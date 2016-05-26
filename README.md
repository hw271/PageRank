This is a project implementing Google's PageRank algorithm on Spark, programming in Scala. We impplement the approach using Wikipedia most recent dump (Wiki2015), about 50 GB. The project contains four subprojects: 

Preprocess: we first extract all the pages from the dump and conduct some text preprocessing.

Filter: then we filter stubs redirects and disambiguation pages.

Rank: We implement the PageRank algorithm in this subproject.

Domain-Specific Rank: We implement domain specific pagerank algorithm in two domains: Football, and Georgetown University. 
