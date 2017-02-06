package com.xuzq.sparkSqlProcess;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

import com.xuzq.sparkSqlThriftClient.SqlClient;
import scala.Tuple2;

public class SparkSqlProcess implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -43920443781873343L;
	private static SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static ArrayList<String> tableList = new ArrayList<String>();
	private static ArrayList<String> dataType = new ArrayList<String>();
	private SparkSession sparkSession = null;

	public SparkSqlProcess(int num) {
		// SparkConf conf = new SparkConf().setAppName("sparksql--" +
		// num).setMaster("local[2]");
		sparkSession = SparkSession.builder().appName("sparksql--" + num).config("spark.cores.max", "32").getOrCreate();
		dataType.add("StringType");
		dataType.add("DateType");
		dataType.add("TimestampType");
	}

	private boolean isInArr(ArrayList<String> tableList, String targetValue) {
		for (String s : tableList) {
			if (s.equals(targetValue))
				return true;
		}
		return false;
	}

	public void sqlAction(SqlClient sqlClient, JSONObject messageJson) {

		System.out.println(ft.format(new Date()) + messageJson.toString());
		// 获取参数
		String sqlType = messageJson.getString("sqlType");
		

		if ("dataView".equals(sqlType)) {
			System.out.println(ft.format(new Date()) + "dataView type");
			// 获取参数
			String key = messageJson.getString("key");
			String userName = messageJson.getString("userName");
			String tableName = messageJson.getString("tableName");
			String sqlString = messageJson.getString("sqlString");
			String dataPath = messageJson.getString("dataPath");
			String sampleNum = messageJson.getString("sampleNum");

			System.out.println(
					ft.format(new Date()) + " key: " + key + " userName: " + userName + " ,tableName: " + tableName
							+ " , sqlString: " + sqlString + ", dataPath: " + dataPath + ", sampleNum: " + sampleNum);

			// 替换将sql语句中的tableName换成userName_tableName,防止不同用户拥有相同的table
			sqlString = sqlString.replaceAll(tableName, userName + "_" + tableName);
			System.out.println(ft.format(new Date()) + " the sqlString is: " + sqlString);

			if (isInArr(tableList, userName + "_" + tableName)) {
				System.out.println(ft.format(new Date()) + " You have the table : " + userName + "_" + tableName);
				try {
					System.out.println(ft.format(new Date()) + " take the the sample num to return");

					// 获取部分结果数据
					String sampleResult = getSampleResultForDV(sqlString, sampleNum);

					System.out.println(ft.format(new Date()) + " the message is " + sampleResult);

					// 将结果返回给前台
					sqlClient.writeResult(key, sampleResult);
				} catch (Throwable e) {
					System.out.println(ft.format(new Date()) + " sqlAction occure error ");
					System.out.println(e);
					JSONObject errorJson = new JSONObject();
					errorJson.put("error", new Throwable().getStackTrace()[0].getLineNumber() + " " + e.getMessage());
					sqlClient.writeResult(key, errorJson.toString());
				}
			} else {
				System.out.println(ft.format(new Date()) + " You don't have the table : " + userName + "_" + tableName);
				try {

					System.out.println(ft.format(new Date()) + " take the the sample num to return");

					// 从hdfs加载数据,并注册成特定的table
					sparkSession.read().parquet(dataPath).createOrReplaceTempView(userName + "_" + tableName);
					tableList.add(userName + "_" + tableName);

					String sampleResult = getSampleResultForDV(sqlString, sampleNum);

					System.out.println(ft.format(new Date()) + " the message is " + sampleResult);

					// 将结果返回给前台
					sqlClient.writeResult(key, sampleResult);

				} catch (Throwable e) {
					System.out.println(ft.format(new Date()) + " sqlAction occure error ");
					System.out.println(e);
					JSONObject errorJson = new JSONObject();
					errorJson.put("error", new Throwable().getStackTrace()[0].getLineNumber() + " " + e.getMessage());
					sqlClient.writeResult(key, errorJson.toString());
				}
			}
		} else {
			System.out.println(ft.format(new Date()) + "sparkSql type");
			
			String filePath = messageJson.getString("filePath");
			Properties readProperties = new Properties();
			try {
				readProperties.load(new FileInputStream(filePath));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				System.out.println(e1);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				System.out.println(e1);
			}
			String db_host = readProperties.getProperty("db_host");
			String db_port = readProperties.getProperty("db_port");
			String db_userName = readProperties.getProperty("db_userName");
			String db_passWord = readProperties.getProperty("db_passWord");
			String db_name = readProperties.getProperty("db_name");
			System.out.println(ft.format(new Date()) + " db_host: " + db_host + " db_port: " + db_port + " ,db_userName: "
					+ db_userName + " , db_passWord: " + db_passWord + ", db_name: " + db_name);
			
			// 获取参数
			String key = messageJson.getString("key");
			String userName = messageJson.getString("userName");
			String tableName = messageJson.getString("tableName");
			String sqlString = messageJson.getString("sqlString");
			String dataPath = messageJson.getString("dataPath");
			String sampleNum = messageJson.getString("sampleNum");
			String resultPath = messageJson.getString("resultPath");
			String resultResp = messageJson.getString("resultResp");
			String queryId = messageJson.getString("queryId");

			System.out.println(ft.format(new Date()) + " key: " + key + " userName: " + userName + " ,tableName: "
					+ tableName + " , sqlString: " + sqlString + ", dataPath: " + dataPath + ", sampleNum: " + sampleNum
					+ ", outputPath: " + resultPath + ", queryId: " + queryId);

			// 替换将sql语句中的tableName换成userName_tableName,防止不同用户拥有相同的table
			sqlString = sqlString.replaceAll(tableName, userName + "_" + tableName);
			System.out.println(ft.format(new Date()) + " the sqlString is: " + sqlString);

			// 判断userName_tableName是否已经被注册过了
			if (isInArr(tableList, userName + "_" + tableName)) {
				System.out.println(ft.format(new Date()) + " You have the table : " + userName + "_" + tableName);
				try {
					// 判断数据路径是否为空
					if ("".equals(resultPath)) {
						System.out.println(ft.format(new Date()) + " the resultPath is null");
						sqlClient.writeResult(key,
								new Throwable().getStackTrace()[0].getLineNumber() + " the resultPath is null");
					} else {
						System.out.println(ft.format(new Date()) + " take the the sample num to return");

						// 获取部分结果数据
						ArrayList<String> sampleResult = getSampleResult(sqlString, sampleNum);

						// 将结果转换成特定格式的json
						JSONObject resultJson = new JSONObject();
						resultJson.put("data", sampleResult);

						System.out.println(ft.format(new Date()) + " the message is " + resultJson.toString());

						// 将结果返回给前台
						sqlClient.writeResult(key, resultJson.toString());

						// 将所有查询结果按照指定的分隔符保存到hdfs上
						saveResultToFile(sqlString, resultPath, resultResp, sampleNum);

						String updateString = String.format(
								"UPDATE spark_sql_histrory_query set save_flag = 'success', job_status = 'success', finish_time = '%s' where auto_id = %s",
								ft.format(new Date()), queryId);
						new MySqlAction(db_host, db_port, db_name, db_userName, db_passWord).update(updateString);
					}
				} catch (Throwable e) {
					System.out.println(ft.format(new Date()) + " sqlAction occure error ");
					e.printStackTrace();
					JSONObject errorJson = new JSONObject();
					errorJson.put("error", new Throwable().getStackTrace()[0].getLineNumber() + " " + e.getMessage());
					sqlClient.writeResult(key, errorJson.toString());
					String updateString = String.format(
							"UPDATE spark_sql_histrory_query set save_flag = 'failure', job_status = 'failure', finish_time = '%s' where auto_id = %s",
							ft.format(new Date()), queryId);
					new MySqlAction(db_host, db_port, db_name, db_userName, db_passWord).update(updateString);
				}
			} else {
				System.out.println(ft.format(new Date()) + " You don't have the table : " + userName + "_" + tableName);
				try {
					if ("Empty".equals(resultPath)) {
						System.out.println(ft.format(new Date()) + " the resultPath is null");
						sqlClient.writeResult(key,
								new Throwable().getStackTrace()[0].getLineNumber() + " the resultPath is null");
					} else {
						System.out.println(ft.format(new Date()) + " take the the sample num to return");

						// 从hdfs加载数据,并注册成特定的table
						sparkSession.read().parquet(dataPath).createOrReplaceTempView(userName + "_" + tableName);
						tableList.add(userName + "_" + tableName);

						// 获取schema
						// String resultSchema = getResultSchems(sqlString);

						// 获取部分结果数据
						ArrayList<String> sampleResult = getSampleResult(sqlString, sampleNum);

						// 将结果转换成特定格式的json
						JSONObject resultJson = new JSONObject();
						// resultJson.put("resultHead", resultSchema);
						resultJson.put("data", sampleResult);
						/*
						 * JSONStringer stringer = new JSONStringer();
						 * stringer.object().key("data").value(sampleResult).
						 * endObject();
						 */
						System.out.println(ft.format(new Date()) + " the message is " + resultJson.toString());

						// 将结果返回给前台
						sqlClient.writeResult(key, resultJson.toString());

						// 将所有查询结果按照指定的分隔符保存到hdfs上
						saveResultToFile(sqlString, resultPath, resultResp, sampleNum);

						String updateString = String.format(
								"UPDATE spark_sql_histrory_query set save_flag = 'success', job_status = 'success', finish_time = '%s' where auto_id = %s",
								ft.format(new Date()), queryId);
						new MySqlAction(db_host, db_port, db_name, db_userName, db_passWord).update(updateString);
					}
				} catch (Throwable e) {
					System.out.println(ft.format(new Date()) + " sqlAction occure error ");
					System.out.println(e);
					JSONObject errorJson = new JSONObject();
					errorJson.put("error", new Throwable().getStackTrace()[0].getLineNumber() + " " + e.getMessage());
					sqlClient.writeResult(key, errorJson.toString());
					String updateString = String.format(
							"UPDATE spark_sql_histrory_query set save_flag = 'failure', job_status = 'failure', finish_time = '%s' where auto_id = %s",
							ft.format(new Date()), queryId);
					new MySqlAction(db_host, db_port, db_name, db_userName, db_passWord).update(updateString);
				}
			}
		}
	}

	// 将所有查询结果按照指定的分隔符保存到hdfs上
	private void saveResultToFile(String sqlString, String resultPath, final String resultResp, String sampleNum) {
		System.out.println(ft.format(new Date()) + " Save head 1000 data into " + resultPath);
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
		sc.parallelize(sparkSession.sql(sqlString).takeAsList(Integer.valueOf(sampleNum))).saveAsTextFile(resultPath);
		/*System.out.println(ft.format(new Date()) + " Save all data into " + resultPath);
		sparkSession.sql(sqlString).write().json(resultPath);*/
		/*
		 * sparkSession.sql(sqlString).toJavaRDD().map(new Function<Row,
		 * String>() { public String call(Row eachRow) throws Exception { return
		 * eachRow.toString().replace("[", "").replace("]", "").replace(",",
		 * resultResp); } }).saveAsTextFile(resultPath);
		 */
	}

	// 获取一部分查询结果,以ArrayList形式返回
	private ArrayList<String> getSampleResult(String sqlString, String sampleNum) {
		ArrayList<String> tmp = new ArrayList<String>();
		List<Row> sqlResult = sparkSession.sql(sqlString).takeAsList(Integer.valueOf(sampleNum));
		for (Row eachRow : sqlResult) {
			int count = 0;
			StringBuffer stringBuffer = new StringBuffer();
			for (; count < eachRow.length() - 1; count++) {
				if (eachRow.get(count) != null) {
					stringBuffer.append(eachRow.get(count).toString());
				}
				else{
					stringBuffer.append("null");
				}
				stringBuffer.append("\u0001");
			}
			if (eachRow.get(count) != null){
				stringBuffer.append(eachRow.get(count).toString());
			}
			else{
				stringBuffer.append("null");
			}
			tmp.add(stringBuffer.toString());
			// tmp.add(eachRow.toString().replace("[", "").replace("]",
			// "").replace(",", "\u0001"));
		}
		return tmp;
	}

	// 获取一部分查询结果,以ArrayList形式返回
	private String getSampleResultForDV(String sqlString, String sampleNum) {
		
		JSONObject resultObj = new JSONObject();
		
		// 获取schema
		ArrayList<Tuple2<String, String>> schemaNameList = getResultSchems(sqlString);
		
		JSONArray headArr = new JSONArray();
		for (Tuple2<String, String> eachSchema : schemaNameList) {
			JSONObject headObj = new JSONObject();
			headObj.put("name", eachSchema._1);
			headObj.put("kname", eachSchema._1);
			headObj.put("type", eachSchema._2);
			headArr.put(headObj);
		}
		
		List<Row> sqlResult = sparkSession.sql(sqlString).takeAsList(Integer.valueOf(sampleNum));
		JSONArray dataArr = new JSONArray();
		for (Row eachRow : sqlResult) {
			int count = 0;
			JSONObject valueObj = new JSONObject();
			for (; count < eachRow.length(); count++) {
				if (eachRow.get(count) != null) {
					valueObj.accumulate(schemaNameList.get(count)._1, eachRow.get(count));
				}
			}
			dataArr.put(valueObj);
		}
		resultObj.put("header", headArr);
		resultObj.put("data", dataArr);
		return resultObj.toString();
	}

	// 获取sql语句查询结果的schema信息
	private ArrayList<Tuple2<String, String>> getResultSchems(String sqlString) {
		//ArrayList<String> schemaNameList = new ArrayList<String>();
		ArrayList<Tuple2<String, String>> schemaNameList = new ArrayList<Tuple2<String, String>>();
		int index = 0;
		StructType schemas = sparkSession.sql(sqlString).schema();
		for (StructField title : schemas.fields()) {
			if(dataType.contains(title.dataType().toString())){
				schemaNameList.add(index, new Tuple2<String, String>(title.name(), "String"));
			}else{
				schemaNameList.add(index, new Tuple2<String, String>(title.name(), "Double"));
			}
			//Tuple2<String, String> tuple2 = new Tuple2<String, String>(title.name(), title.dataType().toString());
			index++;
		}
		return schemaNameList;
	}

	public String getMessageKey(JSONObject message) {
		String key = message.getString("key");
		return key;
	}

	public void stop() {
		sparkSession.stop();
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.out.println(
					"Usage: sparkSqlProcess <sqlProcessNum> <sqlThriftServerHost> <sqlThriftServerPort> <timeOut>");
			System.exit(1);
		}

		String sqlProcessNum = args[0];
		String sqlThriftServerHost = args[1];
		String sqlThrfitServerPort = args[2];
		String timeOut = args[3];

		SparkSqlProcess sqlProcess = new SparkSqlProcess(Integer.valueOf(sqlProcessNum));

		SqlClient sqlClient = new SqlClient(sqlThriftServerHost, Integer.valueOf(sqlThrfitServerPort),
				Integer.valueOf(timeOut));

		try {
			sqlClient.openSocket();

			while (true) {
				String message = sqlClient.getMessage();
				if (!"Empty".equals(message)) {
					System.out.println(ft.format(new Date()) + " " + new Throwable().getStackTrace()[0].getLineNumber()
							+ " the message is: " + message);
					System.out.println("Hello SparkSqlProcess");
					// 删除该进程
					if ("CloseSqlProcess".equals(message)) {
						System.out.println(ft.format(new Date()) + " "
								+ new Throwable().getStackTrace()[0].getLineNumber() + " closesqlProcess");
						break;
					}

					JSONObject messageJson = new JSONObject(message);
					sqlProcess.sqlAction(sqlClient, messageJson);
				}
				Thread.sleep(2000);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			sqlClient.closeSocket();
			sqlProcess.stop();
		}
	}
}
