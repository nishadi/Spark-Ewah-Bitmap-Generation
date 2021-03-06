import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
        spark.sql("USE " + DATABASE_NAME);

        query52_bitmaps(spark);
        query88_bitmaps(spark);

     // Testing for different types of queries
        cat_num_bitmaps_range(spark);
        cat_num_bitmaps(spark);
        or_bitmaps_val(spark);
        or_bitmaps_range(spark);

        distinct(spark);
        bitslices(spark);
        spark.stop();
    }

    public static void sampleBitmapGeneration(SparkSession spark){
        String columnName = "ss_quantity";
        String query = "SELECT " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";
        Projection ss_quantity = new Projection(columnName, query);
        ss_quantity.generateBitmaps(spark);
        ss_quantity.createIndexTable(spark, columnName + "__index");

        columnName = "t_hour";
        query = "SELECT " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";

        Projection hourCol = new Projection(columnName, query);
        hourCol.generateBitmaps(spark);
        hourCol.createIndexTable(spark, columnName + "__index");

        columnName = "hd_dep_count";
        query = "SELECT " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";

        Projection depCountCol = new Projection(columnName, query);
        depCountCol.generateBitmaps(spark);
        depCountCol.createIndexTable(spark, columnName + "__index");

        columnName = "s_store_name";
        query = "SELECT " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";

        Projection storeNameCol = new Projection(columnName, query);
        storeNameCol.generateBitmaps_String(spark);
        storeNameCol.createIndexTable_String(spark, columnName + "__index");

        columnName = "t_minute";
        query = "SELECT " + columnName
                + " FROM store_sales, household_demographics, time_dim, store" +
                " WHERE ss_sold_time_sk = time_dim.t_time_sk and" +
                " ss_hdemo_sk = household_demographics.hd_demo_sk and" +
                " ss_store_sk = s_store_sk";

        Projection minCol = new Projection("t_minute" ,query);
        minCol.generateBitmaps_Range(spark, 30);
        minCol.createIndexTable_String(spark, columnName + "__index");

    }

    public static void distinct(SparkSession spark){
        String columnName = "c_birth_year";
        String selectQuery = "SELECT " + columnName
                + " FROM customer";
        Projection cat_num = new Projection(columnName, selectQuery);
        cat_num.generateBitmaps(spark);
        cat_num.createIndexTable(spark, columnName + "__index");
    }

    public static void bitslices(SparkSession spark){

        String columnName = "cs_quantity";
        String selectQuery = "SELECT " + columnName
                + " FROM catalog_sales";

        BitSlices ss_quantity_bitslices = new BitSlices("ss_quantity", selectQuery);
        ss_quantity_bitslices.generateBitSlices(spark);
        ss_quantity_bitslices.createIndexTable(spark, columnName + "__index");
    }
    public static void cat_num_bitmaps_range(SparkSession spark){
        String columnName = "cp_catalog_number";
        String selectQuery = "SELECT " + columnName
                + " FROM catalog_page, catalog_returns where catalog_page.cp_catalog_page_sk = catalog_returns.cr_catalog_page_sk";
        Projection cat_num = new Projection(columnName, selectQuery);
        cat_num.generateBitmaps_Range(spark, 50);
        cat_num.createIndexTable_String(spark, columnName + "__index");

         columnName = "cr_return_quantity";
         selectQuery = "SELECT " + columnName
                + " FROM catalog_page, catalog_returns where catalog_page.cp_catalog_page_sk = catalog_returns.cr_catalog_page_sk";
        Projection ret = new Projection(columnName, selectQuery);
        ret.generateBitmaps_Range(spark, 40);
        ret.createIndexTable_String(spark, columnName + "__index");

    }

    public static void cat_num_bitmaps(SparkSession spark){
        String columnName = "cp_catalog_number";
        String selectQuery = "SELECT " + columnName
                + " FROM catalog_page, catalog_returns where catalog_page.cp_catalog_page_sk = catalog_returns.cr_catalog_page_sk";
        Projection cat_num = new Projection(columnName, selectQuery);
        cat_num.generateBitmaps(spark);
        cat_num.createIndexTable(spark, columnName + "_val__index");

        columnName = "cr_return_quantity";
        selectQuery = "SELECT " + columnName
                + " FROM catalog_page, catalog_returns where catalog_page.cp_catalog_page_sk = catalog_returns.cr_catalog_page_sk";
        Projection ret = new Projection(columnName, selectQuery);
        ret.generateBitmaps(spark);
        ret.createIndexTable(spark, columnName + "_val__index");

    }

    public static void or_bitmaps_val(SparkSession spark){
        String columnName = "ss_quantity";
        String selectQuery = "SELECT " + columnName
                + " FROM store_sales";
        Projection cat_num = new Projection(columnName, selectQuery);
        cat_num.generateBitmaps(spark);
        cat_num.createIndexTable(spark, columnName + "_val__index");

         columnName = "ss_ext_sales_price";
         selectQuery = "SELECT " + columnName
                + " FROM store_sales";
        Projection ret = new Projection(columnName, selectQuery);
        ret.generateBitmaps_Decimal(spark);
        ret.createIndexTable(spark, columnName + "_val__index");

    }


    public static void or_bitmaps_range(SparkSession spark){
        String columnName = "ss_quantity";
        String selectQuery = "SELECT " + columnName
                + " FROM store_sales";
        Projection cat_num = new Projection(columnName, selectQuery);
        cat_num.generateBitmaps_Range(spark,50);
        cat_num.createIndexTable_String(spark, columnName + "__index");

        columnName = "ss_ext_sales_price";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales";
        Projection ret = new Projection(columnName, selectQuery);
        ret.generateBitmaps_Range_Dec(spark,1000);
        ret.createIndexTable_String(spark, columnName + "__index");

    }

    public static void query52_bitmaps(SparkSession spark) {

        // Projection indexes
        String columnName = "i_manager_id";
        String selectQuery = "SELECT " + columnName
                + " FROM date_dim dt, store_sales, item" +
                " WHERE dt.d_date_sk = store_sales.ss_sold_date_sk and" +
                " store_sales.ss_item_sk = item.i_item_sk";
        Projection i_manager_id = new Projection(columnName, selectQuery);
        i_manager_id.generateBitmaps(spark);
        i_manager_id.createIndexTable(spark, columnName + "_52__index");

        columnName = "d_moy";
        selectQuery = "SELECT " + columnName
                + " FROM date_dim dt, store_sales, item" +
                " WHERE dt.d_date_sk = store_sales.ss_sold_date_sk and" +
                " store_sales.ss_item_sk = item.i_item_sk";
        Projection d_moy = new Projection(columnName, selectQuery);
        d_moy.generateBitmaps(spark);
        d_moy.createIndexTable(spark, columnName + "_52__index");

        columnName = "d_year";
        selectQuery = "SELECT " + columnName
                + " FROM date_dim dt, store_sales, item" +
                " WHERE dt.d_date_sk = store_sales.ss_sold_date_sk and" +
                " store_sales.ss_item_sk = item.i_item_sk";
        Projection d_year = new Projection(columnName, selectQuery);
        d_year.generateBitmaps(spark);
        d_year.createIndexTable(spark, columnName + "_52__index");

        // Bit sliced indexes
        columnName = "ss_ext_sales_price";
        selectQuery = "SELECT " + columnName
                + " FROM date_dim dt, store_sales, item" +
                " WHERE dt.d_date_sk = store_sales.ss_sold_date_sk and" +
                " store_sales.ss_item_sk = item.i_item_sk";

        BitSlices ss_ext_sales_price = new BitSlices(columnName, selectQuery);
        ss_ext_sales_price.generateBitSlices_Decimal(spark);
        ss_ext_sales_price.createIndexTable(spark, columnName + "_52__bs_index");

    }

    public static void query88_bitmaps(SparkSession spark) {

        // Sum query
        // Bit Slices on ss_ quantity
        String query = "SELECT ss_quantity"
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";

        BitSlices ss_quantity_bitslices = new BitSlices("ss_quantity", query);
        ss_quantity_bitslices.generateBitSlices(spark);
        ss_quantity_bitslices.createIndexTable(spark, "ss_quantity__bs_index");

        // Projection indexes
        String columnName = "d_year";
        String selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        Projection d_year = new Projection(columnName, selectQuery);
        d_year.generateBitmaps(spark);
        d_year.createIndexTable(spark, columnName + "__index");

        columnName = "cd_marital_status";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        Projection cd_marital_status = new Projection(columnName, selectQuery);
        cd_marital_status.generateBitmaps_String(spark);
        cd_marital_status.createIndexTable_String(spark, columnName + "__index");

        columnName = "cd_education_status";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        Projection cd_education_status = new Projection(columnName, selectQuery);
        cd_education_status.generateBitmaps_String(spark);
        cd_education_status.createIndexTable_String(spark, columnName + "__index");

        columnName = "ca_country";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        Projection ca_country = new Projection(columnName, selectQuery);
        ca_country.generateBitmaps_String(spark);
        ca_country.createIndexTable_String(spark, columnName + "__index");

        // Between queries
        columnName = "ss_net_profit";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        HashMap<Integer, Integer> betweenMap2 = new HashMap<>();
        betweenMap2.put(0, 2000);
        betweenMap2.put(150, 3000);
        betweenMap2.put(50, 25000);
        Projection ss_net_profit = new Projection(columnName, selectQuery);
        ss_net_profit.setBetweenMap(betweenMap2);
        ss_net_profit.generateBitmaps_Decimal(spark);
        ss_net_profit.createIndexTable_String(spark, columnName + "__index");


        columnName = "ss_sales_price";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        HashMap<Integer, Integer> betweenMap3 = new HashMap<>();
        betweenMap3.put(50, 100);
        betweenMap3.put(100, 150);
        betweenMap3.put(150, 200);
        Projection ss_sales_price = new Projection(columnName, selectQuery);
        ss_sales_price.setBetweenMap(betweenMap2);
        ss_sales_price.generateBitmaps_Decimal(spark);
        ss_sales_price.createIndexTable_String(spark, columnName + "__index");

        // In Queries
        columnName = "ca_state";
        selectQuery = "SELECT " + columnName
                + " FROM store_sales, store, customer_demographics, customer_address, date_dim" +
                " WHERE ss_sold_date_sk = d_date_sk and" +
                " cd_demo_sk = ss_cdemo_sk and" +
                " ss_addr_sk = ca_address_sk and" +
                " ss_store_sk = s_store_sk";
        List<String> inList = new ArrayList<>(Arrays.asList("CO OH TX", "OR MN KY", "VA CA MS"));
        Projection ca_state = new Projection(columnName, selectQuery);
        ca_state.setInList(inList);
        ca_state.generateBitmaps_String(spark);
        ca_state.createIndexTable_String(spark, columnName + "__index");
    }
}
