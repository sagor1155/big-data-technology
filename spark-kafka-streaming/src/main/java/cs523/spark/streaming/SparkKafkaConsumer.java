package cs523.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;

import cs523.spark.streaming.HBaseTable;
import cs523.spark.streaming.Stock;

import java.util.*;


public class SparkKafkaConsumer {
    
    public static void main(String[] args) throws InterruptedException {
        // Set the Spark configuration
        SparkConf conf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(jsc, Durations.seconds(5));

        // initialize HBase Table
        try {
        	HBaseTable.init();	
        } catch(Exception ex) {
        	System.out.println("Couldn't create/open table!");
        	System.out.println(ex.getMessage());
        }
        
        String broker = "localhost:9092";
        Set<String> topics = Collections.singleton("stock");        
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", broker);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "test-group");

        // Create the KAFKA direct stream        
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(streamingContext, 
        			String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        stream.foreachRDD(rdd -> {        	
        	rdd.values()
            	.filter(line -> line!=null && !line.isEmpty())
	            .map(line -> mapToStock(line))
	            .filter(stock -> stock != null)
	            .foreach(stock -> {
	            	System.out.println(stock);
	            	try {
						HBaseTable.insert(stock);
					} catch (Exception e) {
						e.printStackTrace();
					} 
	            });
            System.out.println();
        });
        
        // Start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }
    
    public static Stock mapToStock(String record) {
    	// date,open,high,low,close,volume
    	try {
	    	String[] arr = record.split(",");
	    	Stock stock = new Stock();
	    	stock.setDate(arr[0]);
	    	stock.setOpen(Float.parseFloat(arr[1]));
	    	stock.setHigh(Float.parseFloat(arr[2]));
	    	stock.setLow(Float.parseFloat(arr[3]));
	    	stock.setClose(Float.parseFloat(arr[4]));
	    	stock.setVolume(Float.parseFloat(arr[5]));
	    	return stock;
    	} catch(Exception ex) {
    		System.out.println("Data Parsing Error. Discarding!");
    		return null;
    	}
    }
   
}