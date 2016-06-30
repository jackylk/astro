
## Astro: Fast SQL on HBase using SparkSQL

Astro is fully distributed SQL engine on HBase by leveraging Spark ecosystem, Apache HBase is a distributed Key-Value store of data on HDFS, but its access mechanism is very primitive and only through client-side APIs, means low productivity and often need to write complex programs. SQL accesses to HBase data are available through Map/Reduce or interfaces mechanisms such as Apache Hive and Impala, or some “native” SQL technologies like Apache Phoenix, purpose-built execution engines, and could not utilize complete spark ecosystem
```
Astro =    SQL on HBase + 
	       Fully distributed + 	  	  
	       Spark Ecosystem
```
Learn more about Astro at:[http://huaweibigdata.github.io/astro/](http://huaweibigdata.github.io/astro/)

## Building Astro

Astro is built using [Apache Maven](http://maven.apache.org/).

I. Clone and build HuaweiBigData/astro
```
$ git clone https://github.com/HuaweiBigData/astro
```

II. Go to the root of the source tree
```
$ cd astro
```

III. Build the project
Build without testing
```
$ mvn -DskipTests clean install 
```
Or, build with testing. It will run test suites against a HBase minicluster.
```
$ mvn clean install
```

## Activate Coprocessor and Custom Filter in HBase

First, add the path of spark-hbase jar to the hbase-env.sh in $HBASE_HOME/conf directory, as follows:
```
HBASE_CLASSPATH=$HBASE_CLASSPATH:/spark-hbase-root-dir/target/astro-project-<version>.jar
```
Then, register the coprocessor service 'CheckDirEndPoint' to hbase-site.xml in the same directory, as follows:
```
<property>
    <name>hbase.coprocessor.region.classes</name>
    <value>org.apache.spark.sql.hbase.CheckDirEndPointImpl</value>
</property>
```
(Warning: Don't register another coprocessor service 'SparkSqlRegionObserver' here !)


## Interactive Scala Shell

The easiest way to start using Astro is through the Scala shell:
```
./bin/hbase-sql
```

## Python Shell

First, add the spark-hbase jar to the SPARK_CLASSPATH in the $SPARK_HOME/conf directory, as follows:
```
SPARK_CLASSPATH=$SPARK_CLASSPATH:/astro-root-dir/target/astro-project-<version>.jar
```
Then go to the spark-hbase installation directory and issue
```
./bin/pyspark-hbase
```
A successfull message is as follows:

   You are using Astro!!!
   HBaseSQLContext available as hsqlContext.

To run a python script, the PYTHONPATH environment should be set to the "python" directory of the Spark-HBase installation. For example,
```
export PYTHONPATH=/astro-root-dir/python
```

Note that the shell commands are not included in the Zip file of the Spark release. They are for developers' use only for this version of 0.1.0. Instead, users can use "$SPARK_HOME/bin/spark-shell --packages HuaweiBigData/astro-{version}" for SQL shell or "$SPARK_HOME/bin/pyspark --packages HuaweiBigData/astro-{version}" for Pythin shell.

## Running Tests

Testing first requires [building Astro](#building-astro). Once Astro is built ...

Run all test suites from Maven:
```
mvn -Phbase,hadoop-2.4 test
```
Run a single test suite from Maven, for example:
```
mvn -Phbase,hadoop-2.4 test -DwildcardSuites=org.apache.spark.sql.hbase.BasicQueriesSuite
```
## IDE Setup

We use IntelliJ IDEA for Astro development. You can get the community edition for free and install the JetBrains Scala plugin from Preferences > Plugins.

To import the current Astro project for IntelliJ:

1. Download IntelliJ and install the Scala plug-in for IntelliJ. You may also need to install Maven plug-in for IntelliJ.
2. Go to "File -> Import Project", locate the Astro source directory, and select "Maven Project".
3. In the Import Wizard, select "Import Maven projects automatically" and leave other settings at their default. 
4. Make sure some specific profiles are enabled. Select corresponding Hadoop version, "maven3" and also"hbase" in order to get dependencies.
5. Leave other settings at their default and you should be able to start your development.
6. When you run the scala test, sometimes you will get out of memory exception. You can increase your VM memory usage by the following setting, for example:

```
-XX:MaxPermSize=512m -Xmx3072m
```

You can also make those setting to be the default by setting to the "Defaults -> ScalaTest".

## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Fork and Contribute
Please feel free to ask any questions or queries through "issues" section of "https://github.com/HuaweiBigData/astro" if you are interested in this project. This project is 100% open for everyone, and we are always open to people who want to use the system or contribute to it. 
This guide document introduce [how to contribute to Astro](https://github.com/HuaweiBigData/astro/wiki/How-to-contribute-to-Astro).
