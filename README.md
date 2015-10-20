# `spark-netezza`

A library to load data into Spark SQL DataFrames from IBM Netezza. JDBC is used to automatically trigger the appropriate commands like `SELECT` on Netezza.

This library is more suited to ETL than interactive queries, since large amounts of data could be extracted for each query execution. If you plan to perform many queries against the same Netezza tables then we recommend saving the extracted data in a format such as Parquet.

## Usage

### Data Sources API

You can use spark-netezza via the Data Sources API in Scala, Python or SQL, as follows:

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