# TopWordsCount with Hadoop

## todo
- [ ] remove jdk dependency from pom >> An
- [x] merge utils
- [ ] try htw hadoop cluster >> why errors?
- [ ] write how to get up and running
- [x] write hadoop deployment script

## get started
1. mvn clean package
2. copy resources (and jar) to hadoop: ~~hdfs dfs -put resources~~ scripts/runDockerAndCopyFiles
3. cp into /usr/local/hadoop and run jar: hadoop jar WordCount.jar resources out

