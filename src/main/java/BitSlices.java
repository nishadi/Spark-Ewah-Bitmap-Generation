
import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BitSlices extends BitMaps {

    public BitSlices(String columnName, String selectQuery) {
        super(columnName, selectQuery);
    }

    public BitSlices() {
    }

    public void setBitSlices(int value, int position) {
        char[] binaryIntInStr = Integer.toBinaryString(value).toCharArray();
        int length = binaryIntInStr.length;
        for (int i = length - 1; i >= 0; i--) {
            if (binaryIntInStr[i] == '1') {
                this.setValue(length - 1 - i, position);
            }
        }
    }

    // For fractional values like 100.50
    public void setBitSlices(BigDecimal value, int position) {

        String valueString = value.toPlainString();
        // Storing decimal value * 100
        int intValue = Integer.parseInt(valueString.substring(0, valueString.indexOf('.')) + valueString.substring(valueString.indexOf('.') + 1));
        char[] binaryIntValue = Integer.toBinaryString(intValue).toCharArray();
        ;
        int length = binaryIntValue.length;
        for (int i = length - 1; i >= 0; i--) {
            if (binaryIntValue[i] == '1') {
                this.setValue(length - 1 - i, position);
            }
        }
    }

    public void generateBitSlices(SparkSession spark) {

        List<Row> rows = spark.sql(selectQuery).collectAsList();
        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setBitSlices(row.getInt(0), rowID);
            }
            rowID++;
        }
    }

    public void generateBitSlices_Decimal(SparkSession spark) {

        List<Row> rows = spark.sql(selectQuery).collectAsList();
        int rowID = 1;
        for (Row row : rows) {
            if (!row.anyNull()) {
                setBitSlices(row.getDecimal(0), rowID);
            }
            rowID++;
        }
    }

    public void createIndexTable(SparkSession spark, String tableName) {

        List<BitSliceRecord> records = new ArrayList<>();

        for (Map.Entry<Integer, EWAHCompressedBitmap> entry : bitMaps.entrySet()) {

            UTF8String bitmap = UTF8String.fromString(entry.getValue().toRLWString());
            List<Row> rows = spark.sql("select ewah_bitmap_count('"+bitmap +"')").collectAsList();

            int count = 0;
            for (Row row : rows) {
                if (!row.anyNull()) {
                    count = row.getInt(0);
                }
            }
            count = (int) (count * Math.pow(2, entry.getKey()));

            BitSliceRecord record = new BitSliceRecord();
            record.setValue(entry.getKey());
            record.setBitmap(entry.getValue().toRLWString());
            record.setCount(count);
            records.add(record);
        }
        Dataset<Row> indexTableDF = spark.createDataFrame(records, BitSliceRecord.class);
        indexTableDF.write().saveAsTable(tableName);
        spark.sql("select * from " + tableName).show();

    }
}
