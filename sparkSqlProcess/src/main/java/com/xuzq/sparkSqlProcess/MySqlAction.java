package com.xuzq.sparkSqlProcess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MySqlAction {
	private static SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,S");
	
	private String host;
	private String port;
	private String databaseName;
	private String userName;
	private String passWord;
	
	public MySqlAction(String host, String port, String databaseName, String userName, String passWord){
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.userName = userName;
		this.passWord = passWord;
	}
	
	public Connection getConnection(){
		Connection con = null; //定义一个MYSQL链接对象
        try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			con = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + databaseName, userName, passWord); //链接本地MYSQL
        }catch (Exception e) {
			// TODO: handle exception
        	System.out.println(e);
		}
        return con;
	}
	
	public void update(String sqlString){
		Statement stmt; //创建声明
        try {
        	System.out.println(ft.format(new Date()) + " sqlString is: " + sqlString);
			stmt = getConnection().createStatement();
			//stmt.executeUpdate("UPDATE spark_sql_histrory_query set save_flag = 'success' where auto_id = 34");
			stmt.executeUpdate(sqlString);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    /**
     * 入口函数
     * @param arg
     */
    public static void main(String arg[]) {
    	new MySqlAction("10.2.1.10", "3306", "db_nbds", "nbds_user", "neunbds").update("");
    }
}
