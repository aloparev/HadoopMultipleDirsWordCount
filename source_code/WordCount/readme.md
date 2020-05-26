# TopWordsCount with Hadoop

## todo
- [ ] remove jdk dependency from pom >> An
- [x] merge utils
- [ ] try htw hadoop cluster
- [ ] write how to get up and running
- [ ] write hadoop deployment script

## get started
1. mvn clean package
2. copy resources (and jar) to hadoop: hdfs dfs -put resources
3. run jar: hadoop jar WordCount.jar resources/ out