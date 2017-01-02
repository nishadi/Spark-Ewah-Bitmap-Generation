import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.List;

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

    Column ss_sold_time_sk = new Column();
    List<Row> ss_sold_time_sk_rows = spark.sql("SELECT ss_hdemo_sk FROM store_sales").collectAsList();
//    List<Row> ss_sold_time_sk_rows = spark.sql("SELECT s_store_name FROM store").collectAsList();

    int rowID = 0;
    for (Row ss_sold_time_sk_row : ss_sold_time_sk_rows) {
      if(!ss_sold_time_sk_row.anyNull()){
        ss_sold_time_sk.setValue(ss_sold_time_sk_row.getInt(0),rowID);
      }
      rowID++;
    }
    System.out.println(ss_sold_time_sk.getBitMaps());

    spark.stop();
  }
}
