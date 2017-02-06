package com.xuzq.server;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.xuzq.sparkSqlThrift.SparkSql;
import com.xuzq.sparkSqlThriftImpl.SparkSqlImpl;

public class Server {
    private static SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss,S");
	private int server_port = 0;
	public Server(int port){
		server_port = port;
	}
	public Server(){
		
	}
	public void startServer() {
		try {
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " spark thrift child server start ....");
			
			TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(server_port);
			THsHaServer.Args tnbArgs = new THsHaServer.Args(tnbSocketTransport);
			tnbArgs.transportFactory(new TFramedTransport.Factory());
			tnbArgs.protocolFactory(new TBinaryProtocol.Factory());

			@SuppressWarnings({ "unchecked", "rawtypes" })
			TProcessor tprocessor = new SparkSql.Processor(new SparkSqlImpl());
			tnbArgs.processor(tprocessor);

			// 使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
			TServer server = new THsHaServer(tnbArgs);
			server.serve();
			
		} catch (Exception e) {
			Date dNow = new Date();
			System.out.println(ft.format(dNow) + " Server start error!!!");
			System.out.println(e);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//args = new String[]{"8093"};
		if(args.length < 1){
			System.out.println("Usage: sparkThriftServer <thrift port>");
			System.exit(1);
		}
		
		Server server = new Server(Integer.parseInt(args[0]));
		server.startServer();
	}
}
