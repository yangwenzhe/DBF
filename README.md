# DBF
Introduction
-------------
   This repo holds the source code and scripts for reproducing the key experiments of fast dataset search with earth mover's distance.
   Download our technical report here: https://github.com/yangwenzhe/DBF/xxxx.pdf

Usage
 ======     
1, Download the necessary dependency from ./mvn_denpendency/torch-clus
---------
2, Add dependency in ./pom.xml:
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
 
 
 
