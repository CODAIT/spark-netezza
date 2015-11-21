# `spark-netezza`

A connector to load data into Spark SQL DataFrames from IBM Netezza database.

## Usage

### Specifying Dependencies

You can use spark-shell, spark-sql, pyspark or spark-submit based on your scenario to invoke spark-netezza connector. Dependency on netezza jdbc driver needs to be provided. For example, for spark-shell:

    spark-shell --jars /path/to/spark-netezza-assembly-0.1.jar --driver-class-path /path/to/nzjdbc.jar

If you prefer not to use the fat jar, an extra dependency flag needs to be set:

    --packages org.apache.commons:commons-csv:1.2

### Data Sources API

You can use spark-netezza connector via the Spark Data Sources API in Scala, Java, Python or SQL, as follows:

#### Scala

```scala
import org.apache.spark.sql._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

val opts = Map("url" -> jdbc:netezza://netezzahost:5480/database",
        "user" -> "username",
        "password" -> "password",
        "dbtable" -> "tablename",
        "numPartitions" -> "4")
val df = sqlContext.read.format("com.ibm.spark.netezza")
  .options(opts).load()
```

#### Java

```java
import org.apache.spark.sql.*;

JavaSparkContext sc = // existing SparkContext
SQLContext sqlContext = new SQLContext(sc);

Map<String, String> opts = new HashMap<>();
opts.put("url", "jdbc:netezza://netezzahost:5480/database");
opts.put("user", "username");
opts.put("password", "password");
opts.put("dbtable", "tablename");
opts.put("numPartitions", "4");
DataFrame df = sqlContext.read()
             .format("com.ibm.spark.netezza")
             .options(opts)
             .load();
```

#### Python

```python
from pyspark.sql import SQLContext

sc = # existing SparkContext
sqlContext = SQLContext(sc)

df = sqlContext.read \
  .format('com.ibm.spark.netezza') \
  .option('url','jdbc:netezza://netezzahost:5480/database') \
  .option('user','username') \
  .option('password','password') \
  .option('dbtable','tablename') \
  .option('numPartitions','4') \
  .load()
```

#### SQL

```sql
CREATE TABLE my_table
USING com.ibm.spark.netezza
OPTIONS (
  url 'jdbc:netezza://netezzahost:5480/database',
  user 'username',
  password 'password',
  dbtable 'tablename'
);
```

## Building From Source

### Scala 2.10
spark-netezza build supports Scala 2.10 by default, if Scala 2.11 artifact is needed, please refer to [Scala 2.11](#scala-211) or [Version Cross Build](#version-cross-build)

#### Building General Artifacts
To generate regular binary, in the root directory run:

    sbt package

To generate assembly jar, in the root directory run:

    sbt assembly

The artifacts will be generated to:

    spark-netezza/target/scala-{binary.version}/

### Scala 2.11
To build against scala 2.11, use '++' option with desired version number, for example:

    sbt ++2.11.7 assembly

#### Version Cross Build
This produces artifacts for both scala 2.10 and 2.11:

Start SBT:

     sbt

Run in the SBT shell:

     + package

#### Using sbt-spark-package Plugin
spark-netezza connector supports sbt-spark-package plugin, to publish to local ivy repository, run:

    sbt spPublishLocal
