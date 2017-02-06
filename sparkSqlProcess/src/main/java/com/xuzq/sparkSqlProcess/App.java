package com.xuzq.sparkSqlProcess;

import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		
		System.out.println( new Throwable().getStackTrace()[0].getLineNumber());
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		System.out.println(ft.format(new Date()));
		
		String updateString = String.format("UPDATE spark_sql_histrory_query set save_flag = 'success', job_status = 'success', finsh_time = '%s' where auto_id = %s", ft.format(new Date()), 97);
		
		System.out.println(updateString);
	}
}
