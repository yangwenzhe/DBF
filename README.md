# DBF
Introduction
-------------
   This repo holds the source code and scripts for reproducing the key experiments of fast dataset search with earth mover's distance.
   Download our technical report here: http://shengwang.site/papers/22VLDB-tr.pdf

Usage
 ======     
1, Download the necessary dependency
---------
Please download the 'torch-clus' folder from the above DBF/mvn_denpendency/torch-clus/ directory and put it in your own maven repository, this torch-clus repository provide the implementation of data structures "indexNode" and "Ball tree".

2, Import the dependency 'torch-clus-0.0.1-SNAPSHOT.jar' into the DBF/pom.xml
-------------
    <dependency>
       <groupId>torch-clus</groupId>
       <artifactId>torch-clus</artifactId>
       <version>0.0.1-SNAPSHOT</version>
    </dependency>
    
3, Compile
----------
    mvn clean package
    
4, Run experiment
------------
    java -cp ./target/DBF-1.0-SNAPSHOT.jar ballTree.experiment.Main
 
 
 
