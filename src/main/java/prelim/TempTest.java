package prelim;

import org.apache.spark.sql.*;

/**
 * Created by Dong Young Yoon on 1/17/17.
 */
public class TempTest
{
	public static void main(String[] args)
	{
		String warehouseLocation = "file:///spark-warehouse";
		SparkSession spark = SparkSession
				.builder()
				.appName("dy test")
        .config("spark.sql.warehouse.dir", warehouseLocation)
				//.config("hive.metastore.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
				.getOrCreate();

		//spark.sql("create external table temp (string pageURL, int pageRank, int avgDuration) location '/data_gen/HiBench/Scan/Input/rankings'");
		//spark.sql("SELECT * from temp").show();
    Dataset<Row> df = spark.read().json("/home/dyoon/spark/examples/src/main/resources/people.json");
    df.show();
	}
}
