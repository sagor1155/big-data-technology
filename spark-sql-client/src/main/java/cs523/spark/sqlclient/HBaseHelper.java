package cs523.spark.sqlclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseHelper {
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
	
	public static void init() throws IOException {
		try {
			connection = ConnectionFactory.createConnection(config);
			Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
			System.out.println("Table name: " + tbl.getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static List<Stock> getStockAnalysis() throws IOException {
		List<Stock> stocks = new ArrayList<Stock>();
		
		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.setCaching(10000);
		
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			Stock stock = new Stock();
			for (Cell cell : result.rawCells()) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				
				if(family.equalsIgnoreCase("price")) {
					if(column.equalsIgnoreCase("open")) {
						stock.setOpen(Bytes.toString(CellUtil.cloneValue(cell)));
					}
					else if(column.equalsIgnoreCase("close")) {
						stock.setClose(Bytes.toString(CellUtil.cloneValue(cell)));
					}
					else if(column.equalsIgnoreCase("high")) {
						stock.setHigh(Bytes.toString(CellUtil.cloneValue(cell)));
					}
					else if(column.equalsIgnoreCase("low")) {
						stock.setLow(Bytes.toString(CellUtil.cloneValue(cell)));
					}
				}
				if(family.equalsIgnoreCase("volumn")) {
					if(column.equalsIgnoreCase("volumn")) {
						stock.setVolume(Bytes.toString(CellUtil.cloneValue(cell)));
					}
				}
			}
			stocks.add(stock);
		}
		
		return stocks;
	}
	private static byte[] floatToBytes(float data) {
		return Bytes.toBytes(String.valueOf(data));
	}
}
