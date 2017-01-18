import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BitMaps {

    HashMap<Integer, EWAHCompressedBitmap> bitMaps;
    HashMap<String, EWAHCompressedBitmap> bitMaps_String;

    String columnName;
    String selectQuery;

    public BitMaps(String columnName, String selectQuery) {
        this.columnName = columnName;
        this.selectQuery = selectQuery;
        bitMaps = new HashMap<>(16, 0.9f);
        bitMaps_String = new HashMap<>(16, 0.9f);
    }

    public BitMaps() {
        this.bitMaps = new HashMap<>(16, 0.9f);
        this.bitMaps_String = new HashMap<>(16, 0.9f);
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
            EWAHCompressedBitmap ewahCompressedBitmap = new EWAHCompressedBitmap();
            ewahCompressedBitmap.set(position);
            bitMaps.put(key, ewahCompressedBitmap);
        }
    }

    public void createIndexTable(SparkSession spark, String tableName) {

        List<Record> records = new ArrayList<>();

        for (Map.Entry<Integer, EWAHCompressedBitmap> entry : bitMaps.entrySet()) {

            Record record = new Record();
            record.setValue(entry.getKey());
            record.setBitmap(entry.getValue().toRLWString());
            records.add(record);
        }
        Dataset<Row> indexTableDF = spark.createDataFrame(records, Record.class);
        indexTableDF.write().saveAsTable(tableName);
        spark.sql("select * from " + tableName).show();

    }
}
