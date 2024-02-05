package cs523.spark.sqlclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQLClient {
	public static void main(String[] args) throws InterruptedException, IOException {
		try {
        	HBaseHelper.init();
        	System.out.println("HBases Initialization is done");
        } catch(Exception ex) {
        	System.out.println("Couldn't create/open table!");
        	System.out.println(ex.getMessage());
        }
		
		SparkConf conf= new SparkConf().setAppName("SparkSQL").setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
	 	SparkSession spark = SparkSession
	      .builder()
	      .appName("SparkSQL2")
	      .config(conf)
	      .getOrCreate();
	 	
	 	showStockAnalysis(sc, spark);
	 	
	 	spark.stop();
	    sc.close();
	}
	
	private static void showStockAnalysis(JavaSparkContext sc,SparkSession spark) throws IOException {

	    JavaRDD<Stock> stocksRDD=sc.parallelize(HBaseHelper.getStockAnalysis());
	    
	    String schemaString = "date open high low close volumn";
	    
	    List<StructField> fields = new ArrayList<StructField>();
	    
	    for (String fieldName : schemaString.split(" ")) 
	    {
	    	StructField field = null;
//	    	if (fieldName.equalsIgnoreCase("date")) {
	    		field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
//	    	} else {
//	    		field = DataTypes.createStructField(fieldName, DataTypes.FloatType, true);
//	    	}
	    	
	    	fields.add(field);
	    }
	    StructType schema = DataTypes.createStructType(fields);

	    JavaRDD<Row> rowRDD = stocksRDD.map((Function<Stock, Row>) record -> 
	    {
//	    	if (record.getDate() !=null) {
//	    		return (Row) RowFactory.create(record.getDate() , record.getOpen(),record.getClose(),record.getLow(), record.getHigh(), record.getVolumn());
//	    	}
//			return null;
	    	return (Row) RowFactory.create(record.getDate() , record.getOpen(),record.getClose(),record.getLow(), record.getHigh(), record.getVolumn());
	    });

	    Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
	    dataFrame.createOrReplaceTempView("stocks");
	    //Dataset<Row> stockResult = spark.sql("SELECT date,open,high,low,close,volumn FROM stocks WHERE date != 'NULL'");
	    Dataset<Row> stockResult = spark.sql("SELECT open,high,low,close,volumn FROM stocks WHERE open != 'NULL'");
	    stockResult.show(10);
	    
	    System.out.println("Select query done for result !!!");

	    //Dataset<Row> stockCount = spark.sql("SELECT date,open,high,low,close,volumn FROM stocks group by date");
	    Dataset<Row> stockCount = spark.sql("SELECT COUNT(open),COUNT(high),COUNT(low),COUNT(close),volumn FROM stocks group by volumn");
	    stockCount.show(10);
	    
	    System.out.println("Count query done for result !!!");
	    stockResult.write().mode("append").option("header","true").csv("hdfs://localhost/user/cloudera/StockSelection");
	    stockCount.write().mode("append").option("header","true").csv("hdfs://localhost/user/cloudera/StockCountAnalysis");
	    
	    System.out.println("HDFS File write done");
	  }
}
