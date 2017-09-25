package com.test.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		/*try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"/home/brijesh/Desktop/cvsXmlLog.json"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = fixJSON(line);
				Gson gson = new Gson();
				JsonElement element = gson.fromJson(line, JsonElement.class);
				JsonObject object = element.getAsJsonObject();
				Iterator<String> it = object.keySet().iterator();
				String id = UUID.randomUUID().toString();
				Put p = new Put(Bytes.toBytes(id));
				while (it.hasNext()) {
					String key = it.next();
					System.out.println(key + " : "+ object.get(key).toString());
					System.out.println(key + " : "+ object.get(key).getAsString());
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}
	private static String fixJSON(String line) {
		return line.replace("][", ",\n").replace("[", "").replace("]", "")
				.replace("}},", "}}");
	}
}
