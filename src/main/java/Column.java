import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Column {

    HashMap<Integer, EWAHCompressedBitmap> bitMaps;
    EWAHCompressedBitmap ewahCompressedBitmap;
    HashMap<String, EWAHCompressedBitmap> bitMaps_String;
    String bitmapString;
    String tableName;
    String columnName;
    String joinCondition;

    public Column(String colName, String tabName, String join) {
        bitMaps = new HashMap<Integer, EWAHCompressedBitmap>(16, 0.9f);
        ewahCompressedBitmap = new EWAHCompressedBitmap();
        bitMaps_String = new HashMap<String, EWAHCompressedBitmap>();
        bitmapString = " ";
        tableName = tabName;
        columnName = colName;
        join = joinCondition;
    }

    public void setValue(String key, int position) {

        // If the key already exists, get the corresponding bitmap and set the value
        if (bitMaps_String.containsKey(key)) {
            bitMaps_String.get(key).set(position);
        }
        // Else Create a new bitmap and add it
        else {
            EWAHCompressedBitmap ewahCompressedBitmap = new EWAHCompressedBitmap();
            ewahCompressedBitmap.set(position);
            bitMaps_String.put(key, ewahCompressedBitmap);
        }
    }

    public void setValue(int key, int position) {
        // If the key already exists, get the corresponding bitmap and set the value
        if (bitMaps.containsKey(key)) {
            bitMaps.get(key).set(position);
        }
        // Else Create a new bitmap and add it
        else {
            ewahCompressedBitmap = new EWAHCompressedBitmap();
            ewahCompressedBitmap.set(position);
            bitMaps.put(key, ewahCompressedBitmap);
        }
    }

    public void generateBitmaps(SparkSession spark) {

        List<Row> rows = spark.sql("SELECT " + columnName + " FROM store_sales, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk").collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setValue(row.getInt(0), rowID);
            }
            rowID++;
        }
    }

    public void generateBitmaps_String(SparkSession spark) {

        List<Row> rows = spark.sql("SELECT " + columnName + " FROM store_sales, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk").collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setValue(row.getString(0), rowID);
            }
            rowID++;
        }

    }

    public void createIndexTable(SparkSession spark) {

        List<Record> records = new ArrayList<>();

        for (Map.Entry<Integer, EWAHCompressedBitmap> entry : bitMaps.entrySet()) {

            Record record = new Record();
            record.setValue(entry.getKey());
            record.setBitmap(entry.getValue().toRLWString());
            records.add(record);
        }
        Dataset<Row> indexTableDF = spark.createDataFrame(records, Record.class);
        indexTableDF.write().saveAsTable(columnName + "__index");
        spark.sql("select * from " + columnName + "__index").show();

    }

    public void createIndexTable_String(SparkSession spark) {

        List<StringRecord> records = new ArrayList<>();

        for (Map.Entry<String, EWAHCompressedBitmap> entry : bitMaps_String.entrySet()) {

            StringRecord record = new StringRecord();
            record.setValue(entry.getKey());
            record.setBitmap(entry.getValue().toRLWString());
            records.add(record);
        }
        Dataset<Row> indexTableDF = spark.createDataFrame(records, StringRecord.class);
        indexTableDF.write().saveAsTable(columnName + "__index");
        spark.sql("select * from " + columnName + "__index").show();

    }

}
