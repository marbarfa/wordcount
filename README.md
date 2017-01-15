wordcount
=========

Simple WordCount example with dum HBase integration written in Scala.
This project was used to test if the 
hadoop + hbase configuration was up and running.

Tested in:
hadoop 2.2 and hbase 0.98.

Some jars need to be added to the classpath by exporting *HADOOP_CLASSPATH* environment 
variable in hadoop-env.sh

### Example:


```bash
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_HOME/lib/hbase-hadoop2-compat-0.98.0-hadoop2.jar:$HBASE_HOME/lib/hbase-client-0.98.0-hadoop2.jar:$HBASE_HOME/lib/hbase-common-0.98.0-hadoop2.jar:$HBASE_HOME/lib/zookeeper-3.4.5.jar:$HBASE_HOME/lib/hbase-protocol-0.98.0-hadoop2.jar:$HBASE_HOME/lib/htrace-core-2.04.jar
```
