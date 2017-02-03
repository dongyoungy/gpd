package prelim;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Dong Young Yoon on 2/2/17.
 */
public class HiveTableSampler
{
  public static void main(String[] args) throws SQLException {
    if (args.length != 1) {
      System.out.println("USAGE: HiveTableSampler <data_dir>");
      System.exit(-1);
    }

    String dataDir = args[0];
//    String tableName = args[1];
//    String sampleTableName = args[2];
    //long sampleSize = Long.parseLong(args[2]);

    String applicationName = "HiveTableSampler";
    String uservisitsData = String.format("%s/uservisits", dataDir);
    String rankingsData = String.format("%s/rankings", dataDir);

    // target sample size = 1m, 50m, 200m, 500m, 1g
    String[] sampleSizeStr = {"1m", "50m", "200m", "500m", "1g"};
    long[] uservisitsSampleSize = {5000, 250000, 1000000, 2500000, 5000000};
    long[] rankingsSampleSize = {600, 30000, 120000, 300000, 600000};

    Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    Statement stmt = conn.createStatement();

    stmt.execute("DROP TABLE IF EXISTS uservisits");
    stmt.execute("DROP TABLE IF EXISTS rankings");
    stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s'", uservisitsData));
    stmt.execute(String.format("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s'", rankingsData));

    int idx = 0;
    while (idx < sampleSizeStr.length) {
      String sizeStr = sampleSizeStr[idx];
      long uservisitsSize = uservisitsSampleSize[idx];
      long rankingsSize = rankingsSampleSize[idx];

      String uservisitsSampleOutput = String.format("%s/sample_%s/uservisits", dataDir, sizeStr);
      String rankingsSampleOutput = String.format("%s/sample_%s/rankings", dataDir, sizeStr);

      stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_sample_%s (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s'", sizeStr, uservisitsSampleOutput));
      stmt.execute(String.format("CREATE EXTERNAL TABLE rankings_sample_%s (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s'", sizeStr, rankingsSampleOutput));

      stmt.execute(String.format("INSERT OVERWRITE TABLE uservisits_sample_%s SELECT sourceIP, destURL, visitDate, adRevenue, userAgent, countryCode, languageCode, searchWord, duration FROM uservisits distribute by rand() sort by rand() limit %d", sizeStr, uservisitsSize));
      stmt.execute(String.format("INSERT OVERWRITE TABLE rankings_sample_%s SELECT pageURL, pageRank, avgDuration FROM rankings distribute by rand() sort by rand() limit %d", sizeStr, rankingsSize));

      ++idx;
    }
  }
}
