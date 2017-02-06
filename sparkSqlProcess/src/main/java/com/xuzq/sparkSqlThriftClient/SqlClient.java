package com.xuzq.sparkSqlThriftClient;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.xuzq.sparkSqlThriftFile.SparkSql;


public class SqlClient implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1079527382683202869L;
	
	private static SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss,S");
	private TTransport transport = null;
	private SparkSql.Client client = null;
	
	public SqlClient(String serverHost, int serverPort, int timeOut){
		transport = new TFramedTransport(new TSocket(serverHost, serverPort, timeOut));
		TProtocol protocol = new TBinaryProtocol(transport);  
        client = new SparkSql.Client(protocol);
	}
	
	public boolean openSocket(){
		try {
			transport.open();
			return  true;
		} catch (TTransportException e) {
			System.out.println(e);
			return false;
		}
	}
	
	public String getMessage(){
		String message;
		try {
			message = client.getMessFromQueue();
			return message;
		} catch (TException e) {
			System.out.println(ft.format(new Date()) + " get the message from Queue occure exception");
			System.out.println(e);
			return e.getMessage();
		}
	}
	
	public String writeResult(String key, String resultMessage){
		try {
			String message = client.addMessIntoMap(key, resultMessage);
			if("OK".equals(message)){
				return "OK";
			}else{
				System.out.println(ft.format(new Date()) + " the add message into map error, the message is: " + message);
				return message;
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			System.out.println(ft.format(new Date()) + " the add message into map occure exception");
			System.out.println(e);
			return e.getMessage();
		}
	}
	
	public void closeSocket(){
		transport.close();
	} 

}
