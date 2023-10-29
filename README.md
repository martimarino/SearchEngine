# SearchEngine-InvertedIndex

This project deals with the creation of a search engine through which a user can request information by using queries that return a list of documents. The relevance of the documents is computed basing on score determined with different metrics. 
The main parts of the project are: 
1. [Index Building](#index)
2. [Query Processing](#query)
3. [Performance Evaluation](#performance)

## Index Building

To generate the index, the file "collection.tsv", available at https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020, must be placed inside *src/main/resources*. The index building starts executing the Main.java file in the path *src/main/java/it/unipi/dii/aide/mircv*.
After the program launch, the user sets out his preferences about: 
- compression
- stopwords and stemming
- debug mode
aswering to the questions displayed by the command line interface.
 After the construction of the index, the following files are saved into *src/main/resources/merged*:
-  dictionary
-  docid
-  termFreq
-  documentTable
-  skipInfo
- collectionStatistics
- flags
All the files are used in the query phase.

## Query Processing
Executing the file Main.java as before, after the index build, the user can insert the query choosing between different options displaues by the command line interface: 
- Conjunctive (pressing *c*) or disjunctive (pressing *d*) queries
- DAAT (pressing *d*) or MaxScore (pressing *m*) algorithm
- TFIDF (pressing *t*) or BM25 (pressing *b*)
- the number of document to obtain
Subsequently the top k documents are returned.

## Performance Evaluation
To evaluate the performance of the system, the *msmarco-test2020-queries.tsv*, that can be downloaded from *https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020* must be added to *src/main/resources*.
The performance test starts with the execution of the file Test.java in *src/main/java/it/unipi/dii/aide/mircv/test*. The user must choose between the options displayed by the command line interface, reguarding: 
- Conjunctive or disjunctive queries
- DAAT or MaxScore
- TFIDF or BM25
- Number of results to retrieve
The program shows the execution time of each query and the average response time. 
These results are saved into files with different names depending on the selected options in the path *src/main/resources/performance*.
