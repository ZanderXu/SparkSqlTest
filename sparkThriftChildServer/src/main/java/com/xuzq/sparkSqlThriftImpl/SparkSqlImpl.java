package com.xuzq.sparkSqlThriftImpl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


import com.xuzq.sparkSqlThrift.*;

public class SparkSqlImpl implements SparkSql.Iface {

	private static SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss,S");
	private static Queue<String> messageQueue = new ConcurrentLinkedQueue<String>();
	private static Map<String, String> resultMap = new ConcurrentHashMap<String, String>();

	public SparkSqlImpl() {
		
	}

	public String addMessIntoQueue(String message) {
		try {
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " add the message into Queue is: " + message);
			messageQueue.add(message);
			return "OK";
		} catch (Exception e) {
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " add the message into Queue Error");
			System.out.println(e);
			return "Faile";
		}
	}

	public String getMessFromQueue() {
		String message = messageQueue.poll();
		if(message == null){
			return "Empty";
		}else {
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " get the message from Queue is: " + message);
			return message;
		}
		
	}

	public String addMessIntoMap(String key, String value) {
		try {
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " add the message into Map is: " + key + " : " + value);
			resultMap.put(key, value);
			return "OK";
		} catch (Exception e) {
			// TODO: handle exception
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " add the message into Map Error");
			System.out.println(e);
			return e.getMessage();
		}
	}

	public String getMessFromMap(String key) {
		try {
			//String resultMessage = resultMap.getOrDefault(key, "Empty");
			String resultMessage = resultMap.get(key);
			//if (!"Empty".equals(resultMessage)){
			if(resultMessage != null){
				Date dNow = new Date();
				System.out.println(ft.format(dNow) + " get the message from Map is: " + key + " : " + resultMessage);
				return resultMessage;
			}else{
				return "Empty";
			}
		} catch (Exception e) {
			// TODO: handle exception
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " get the message from Map Error");
			System.out.println(e);
			return e.getMessage();
		}
	}

}
