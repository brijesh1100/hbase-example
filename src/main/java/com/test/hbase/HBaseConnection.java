package com.test.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class HBaseConnection {
	public static String TABLE_NAME = "TESTLOGS";
	public static String COLUMN_FAMILY = "ANALYTICS";

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		// zookeeper-
		// ip-172-17-0-107.ap-southeast-1.compute.internal,ip-172-17-0-112.ap-southeast-1.compute.internal
		conf.set("hbase.master",
				"ip-XXX-XX-X-XXX.ap-southeast-1.compute.internal");
		conf.set("hbase.rootdir",
				"hdfs://ip-XXX-XX-X-XXX.ap-southeast-1.compute.internal:8020/apps/hbase/data");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set(
				"hbase.zookeeper.quorum",
				"ip-XXX-XX-X-XXX.ap-southeast-1.compute.internal,ip-XXX-XX-X-XXX.ap-southeast-1.compute.internal");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			HTableDescriptor descriptor = new HTableDescriptor(
					TableName.valueOf(HBaseConnection.TABLE_NAME));
			descriptor.addFamily(new HColumnDescriptor(HBaseConnection.COLUMN_FAMILY));
			if (!admin.isTableAvailable(descriptor.getTableName())) {
				System.out.println("Creating Table");
				admin.createTable(descriptor);
				System.out.println("Table creation has been Done.");
			}

			try {
				BufferedReader reader = new BufferedReader(
						new FileReader(
								"/home/brijesh/testLog.json"));
				String line = null;
				while ((line = reader.readLine()) != null) {
					line = fixJSON(line);
					Gson gson = new Gson();
					JsonElement element = gson
							.fromJson(line, JsonElement.class);
					JsonObject object = element.getAsJsonObject();
					Iterator<String> it = object.keySet().iterator();
					table = connection.getTable(TableName.valueOf(HBaseConnection.TABLE_NAME));
					String id = UUID.randomUUID().toString();
					Put p = new Put(Bytes.toBytes(id));
					while (it.hasNext()) {
						String key = it.next();
						p.addColumn(Bytes.toBytes(HBaseConnection.COLUMN_FAMILY),
								Bytes.toBytes(key),
								Bytes.toBytes(object.get(key).getAsString()));
					}
					table.put(p);
					Result r = table.get(new Get(Bytes.toBytes(id)));
					System.out.println("\n"+r);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// close everything down
			if (table != null) {
				table.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}

	private static String fixJSON(String line) {
		return line.replace("][", ",\n").replace("[", "").replace("]", "")
				.replace("}},", "}}");
	}
}
