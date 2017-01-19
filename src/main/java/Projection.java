import java.util.*;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Projection extends BitMaps {

    HashMap<Integer, EWAHCompressedBitmap> bitMaps;
    HashMap<String, String> stringHashMap;
    EWAHCompressedBitmap ewahCompressedBitmap;
    String bitmapString;
    int rangeBreakPoint;
    boolean multipleBreakPoints;
    int rangeBreakPoint2;
    int rangeBreakPoint3;
    HashMap<Integer, Integer> betweenMap;
    List<String> inList;
    boolean betweenQuery;
    boolean inQuery;


    public Projection(String colName, String query) {
        bitMaps = new HashMap<>(16, 0.9f);
        ewahCompressedBitmap = new EWAHCompressedBitmap();
        bitMaps_String = new HashMap<>();
        bitmapString = " ";
        columnName = colName;
        multipleBreakPoints = false;
        selectQuery = query;
        betweenMap = null;
        betweenQuery = false;
        inList = null;
        inQuery = false;
    }

    public void setBetweenMap(HashMap<Integer, Integer> betweenMap) {
        this.betweenMap = betweenMap;
        betweenQuery = true;
    }

    public void setInList(List<String> inList) {
        this.inList = inList;
        this.inQuery = true;
    }

    public void setProjectionValue(int key, int position) {

        if (betweenQuery) {
            String betweenKey = null;
            for (Map.Entry<Integer, Integer> entry : betweenMap.entrySet()) {

                int lowerLimit = entry.getKey();
                int upperLimit = entry.getValue();

                if (key >= lowerLimit && key <= upperLimit) {
                    betweenKey = lowerLimit + "to" + upperLimit;
                    setValue(betweenKey, position);
                }
            }
        } else {
            setValue(key, position);
        }
    }

    public void setProjectionValue(String key, int position) {

        if (inQuery) {
            for (String inString : inList) {
                if (inString.contains(key)) {
                    setValue(inString, position);
                }
            }
        } else {
            setValue(key, position);
        }
    }

    public void generateBitmaps(SparkSession spark) {

        List<Row> rows = spark.sql(selectQuery).collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setProjectionValue(row.getInt(0), rowID);
            }
            rowID++;
        }
    }

    public void generateBitmaps_Decimal(SparkSession spark) {

        List<Row> rows = spark.sql(selectQuery).collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setProjectionValue(row.getDecimal(0).intValue(), rowID);
            }
            rowID++;
        }
    }

    public void generateBitmaps_String(SparkSession spark) {

        List<Row> rows = spark.sql(selectQuery).collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setProjectionValue(row.getString(0), rowID);
            }
            rowID++;
        }
    }


    public void createIndexTable_String(SparkSession spark, String tableName) {

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

    // Range
    public void setValueRange(int keyInt, int position) {

        String key = null;
        if (multipleBreakPoints) {
            if (keyInt <= rangeBreakPoint) {
                key = "<=" + rangeBreakPoint;
                this.setValue(key, position);
            }
            if (keyInt <= rangeBreakPoint2) {
                key = "<=" + rangeBreakPoint2;
                this.setValue(key, position);
            }
            if (keyInt <= rangeBreakPoint3) {
                key = "<=" + rangeBreakPoint3;
                this.setValue(key, position);
            }
        } else {
            if (keyInt >= rangeBreakPoint)
                key = ">=" + rangeBreakPoint;
            else
                key = "<" + rangeBreakPoint;
            this.setValue(key, position);
        }
    }

    public void generateBitmaps_Range(SparkSession spark, int rangeBreakPoint) {

        this.rangeBreakPoint = rangeBreakPoint;

        String query = "SELECT monotonically_increasing_id() as rowid, " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";

        List<Row> rows = spark.sql("SELECT " + columnName + " FROM store_sales, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk").collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setValueRange(row.getInt(0), rowID);
            }
            rowID++;
        }
    }

    public void generateBitmaps_Range(SparkSession spark, int rangeBreakPoint1, int rangeBreakPoint2, int rangeBreakPoint3) {

        multipleBreakPoints = true;
        this.rangeBreakPoint = rangeBreakPoint1;
        this.rangeBreakPoint2 = rangeBreakPoint2;
        this.rangeBreakPoint3 = rangeBreakPoint3;

        String query = "SELECT monotonically_increasing_id() as rowid, " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";

        List<Row> rows = spark.sql("SELECT " + columnName + " FROM store_sales, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk").collectAsList();

        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setValueRange(row.getInt(0), rowID);
            }
            rowID++;
        }
    }

}
