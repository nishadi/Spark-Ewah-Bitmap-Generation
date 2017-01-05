import org.apache.spark.sql.SparkSession;

public class JavaEwahBitmapGeneration {

  public static void main(String[] args) {

    String warehouseLocation = "file:" + System.getProperty("user.dir") + "spark-warehouse";
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();

    final String DATABASE_NAME = "finaltest";
    spark.sql("USE "+DATABASE_NAME);

    String tableNames = "store_sales, household_demographics, time_dim, store";
    String whereCondition = "ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk";

    Column hourCol = new Column("t_hour" , tableNames, whereCondition);
    hourCol.generateBitmaps(spark);
    hourCol.createIndexTable(spark);

    Column depCountCol = new Column("hd_dep_count" , tableNames, whereCondition);
    depCountCol.generateBitmaps(spark);
    depCountCol.createIndexTable(spark);

    Column storeNameCol = new Column("s_store_name" , tableNames, whereCondition);
    storeNameCol.generateBitmaps_String(spark);
    storeNameCol.createIndexTable_String(spark);

    spark.stop();
  }
}
