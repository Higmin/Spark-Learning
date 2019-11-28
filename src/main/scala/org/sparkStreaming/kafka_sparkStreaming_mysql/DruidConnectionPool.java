package org.sparkStreaming.kafka_sparkStreaming_mysql;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import javax.sql.DataSource;
import java.util.Properties;

/**
 * @author Higmin
 * @date 2019/11/28 9:10
 **/
public class DruidConnectionPool {

	public DataSource dataSource;
	private Properties pro = new Properties();

	private DruidConnectionPool() {
		try {
			init();
			dataSource = DruidDataSourceFactory.createDataSource(pro);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class Holder {
		private static DruidConnectionPool instance = new DruidConnectionPool();
	}

	public static DruidConnectionPool getInstance() {
		return Holder.instance;
	}

	private void init() {
		// 数据源配置
		pro.setProperty("driverClassName", "com.mysql.jdbc.Driver");
		pro.setProperty("url", "jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=true");
		pro.setProperty("username", "root");
		pro.setProperty("password", "root");
		// 连接池配置
		pro.setProperty("initialSize", "20"); // 初始化连接大小
		pro.setProperty("minIdle", "20"); // 最小连接池数量
		pro.setProperty("maxActive", "100"); // 最大连接池数量
		pro.setProperty("maxWait", "60000"); // 获取连接时最大等待时间，单位毫秒
		pro.setProperty("timeBetweenEvictionRunsMillis", "60000"); // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
		pro.setProperty("minEvictableIdleTimeMillis", "300000"); // 配置一个连接在池中最小生存的时间，单位是毫秒
		pro.setProperty("validationQuery", "SELECT 1 FROM DUAL"); // 测试连接
		pro.setProperty("testWhileIdle", "true"); // 申请连接的时候检测，建议配置为true，不影响性能，并且保证安全性
		pro.setProperty("testOnBorrow", "false"); // 获取连接时执行检测，建议关闭，影响性能
		pro.setProperty("testOnReturn", "false"); // 归还连接时执行检测，建议关闭，影响性能
		pro.setProperty("poolPreparedStatements", "false");        // 是否开启PSCache，PSCache对支持游标的数据库性能提升巨大，oracle建议开启，mysql下建议关闭
		pro.setProperty("maxOpenPreparedStatements", "20"); // 开启poolPreparedStatements后生效
//		pro.setProperty("filters", "stat,wall,slf4j"); // 配置扩展插件，常用的插件有=>stat:监控统计  log4j:日志  wall:防御sql注入
		pro.setProperty("connectionProperties", "druid.stat.mergeSql=true;druid.stat.slowSqlMillis=5000"); // 通过connectProperties属性来打开mergeSql功能;慢SQL记录
		pro.setProperty("asyncInit", "true");
	}

}
