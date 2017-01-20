package prelim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Dong Young Yoon on 1/17/17.
 */
public class TempTestCached
{
  public static void main(String[] args)
  {
    String workload = args[0];
    String dataSize = args[1];
    String execMem = args[2];
    String applicationName = workload + "_" + dataSize + "_cached_" + execMem;
    String inputData = String.format("/data_gen/HiBench/Data/%s/Input", dataSize);
    String outputData = String.format("/data_gen/HiBench/Data/%s/Output", dataSize);
    String warehouseLocation = "file:///spark-warehouse";
    SparkSession spark = SparkSession
        .builder()
        .appName(applicationName)
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate();

    switch (workload)
    {
      case "Scan":
      {
        // setup queries
        spark.sql("UNCACHE TABLE uservisits");
        spark.sql("DROP TABLE IF EXISTS uservisits");
        spark.sql(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        spark.sql("DROP TABLE IF EXISTS uservisits_copy");
        spark.sql(String.format("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits_copy'", outputData));

        // get plan
        Dataset<Row> plan = spark.sql("EXPLAIN EXTENDED INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits");
        plan.write().mode("overwrite").text(String.format("/query_plans/%s/%s/mem_%s", workload, dataSize, execMem));

        // create cache
        spark.sql("CACHE TABLE uservisits");
        Dataset<Row> plan_cached = spark.sql("EXPLAIN EXTENDED INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits");
        plan_cached.write().mode("overwrite").text(String.format("/query_plans/%s/%s_cached/mem_%s", workload, dataSize, execMem));

        // run query
        spark.sql("INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits");

        break;
      }
      case "Join":
      {
        // setup queries
        spark.sql("UNCACHE TABLE rankings");
        spark.sql("UNCACHE TABLE uservisits_copy");
        spark.sql("DROP TABLE IF EXISTS rankings");
        spark.sql(String.format("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/rankings'", inputData));
        spark.sql("DROP TABLE IF EXISTS uservisits_copy");
        spark.sql(String.format("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        spark.sql("DROP TABLE IF EXISTS rankings_uservisits_join");
        spark.sql(String.format("CREATE EXTERNAL TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE ) STORED AS  SEQUENCEFILE LOCATION '%s/rankings_uservisits_join'", outputData));

        // get plan
        Dataset<Row> plan = spark.sql("EXPLAIN EXTENDED INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        plan.write().mode("overwrite").text(String.format("/query_plans/%s/%s/mem_%s", workload, dataSize, execMem));

        // create cache
        spark.sql("CACHE TABLE uservisits_copy");
        spark.sql("CACHE TABLE rankings");
        Dataset<Row> plan_cached = spark.sql("EXPLAIN EXTENDED INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        plan_cached.write().mode("overwrite").text(String.format("/query_plans/%s/%s_cached/mem_%s", workload, dataSize, execMem));

        // run query
        spark.sql("INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        break;
      }
      case "Aggregation":
      {
        // setup queries
        spark.sql("UNCACHE TABLE uservisits");
        spark.sql("DROP TABLE IF EXISTS uservisits");
        spark.sql(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        spark.sql("DROP TABLE IF EXISTS uservisits_aggre");
        spark.sql(String.format("CREATE EXTERNAL TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE ) STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'", outputData));

        // get plan
        Dataset<Row> plan = spark.sql("EXPLAIN EXTENDED INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        plan.write().mode("overwrite").text(String.format("/query_plans/%s/%s/mem_%s", workload, dataSize, execMem));

        // create cache
        spark.sql("CACHE TABLE uservisits");
        Dataset<Row> plan_cached = spark.sql("EXPLAIN EXTENDED INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        plan_cached.write().mode("overwrite").text(String.format("/query_plans/%s/%s_cached/mem_%s", workload, dataSize, execMem));

        // run query
        spark.sql("INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        break;
      }
      default:
        return;
    }

    spark.stop();
  }
}
