package cs523.spark.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTable {

	private static Configuration config = HBaseConfiguration.create();
	private static Connection connection = null;
	
	private static final String TABLE_NAME = "stocks";
	
	private static final byte[] CF_PRICE = Bytes.toBytes("price");
	private static final byte[] CF_VOLUMN = Bytes.toBytes("volumn");
	
	private static final byte[] COL_OPEN = Bytes.toBytes("open");
	private static final byte[] COL_HIGH = Bytes.toBytes("high");
	private static final byte[] COL_LOW = Bytes.toBytes("low");
	private static final byte[] COL_CLOSE = Bytes.toBytes("close");
	private static final byte[] COL_VOLUMN = Bytes.toBytes("volumn");

	// Columns: date,open,high,low,close,volume
	// RowKey: date
	// CF1: Price (open, high, low, close)
	// CF2: Volumn (volumn)
	
	
	public static void init() throws IOException {
		try {
			connection = ConnectionFactory.createConnection(config);
			Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
			System.out.println("Table name: " + tbl.getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void create() throws IOException {
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			TableName tableName = TableName.valueOf(TABLE_NAME);
			
			TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
			ColumnFamilyDescriptor cfdPrice = ColumnFamilyDescriptorBuilder.newBuilder(CF_PRICE).build();
			ColumnFamilyDescriptor cfdVolumn = ColumnFamilyDescriptorBuilder.newBuilder(CF_VOLUMN).build();
			TableDescriptor tableDescriptor = tableBuilder
					.setColumnFamily(cfdPrice)
					.setColumnFamily(cfdVolumn)
					.build();
				
			System.out.println("Table Initialize");
			if (!admin.tableExists(tableName)) {
				System.out.println("Table doesn't exists! Creating table....");
				admin.createTable(tableDescriptor);
			} else {
				System.out.println("Table already exists");
			}
			
			System.out.println("Done!");
		}
	}	

	public static void insert(Stock stock) throws IOException {
		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		
		System.out.print("Inserting data.... ");
		try {
			Put p = new Put(Bytes.toBytes(stock.getDate()));
			
			p.addColumn(CF_PRICE, COL_OPEN, floatToBytes(stock.getOpen()));
			p.addColumn(CF_PRICE, COL_HIGH, floatToBytes(stock.getHigh()));
			p.addColumn(CF_PRICE, COL_LOW, floatToBytes(stock.getLow()));
			p.addColumn(CF_PRICE, COL_CLOSE, floatToBytes(stock.getClose()));
			
			p.addColumn(CF_VOLUMN, COL_VOLUMN, floatToBytes(stock.getVolumn()));
		
			table.put(p);
			System.out.println("Done");
		} finally {
			table.close();
		}
	}
	
	private static byte[] floatToBytes(float data) {
		return Bytes.toBytes(String.valueOf(data));
	}

}
