
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.List;

public class BitSlices extends BitMaps {

    public BitSlices(String columnName, String selectQuery) {
        super(columnName, selectQuery);
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
}
