package org.sparkStreaming.kafka_sparkStreaming_mysql;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author Higmin
 * @date 2019/11/28 9:10
 **/
public class DruidConnectionPool {

	private static DruidDataSource datasource;

	/**
	 * 单例创建连接池对象
	 * @return
	 */
//	@Bean
//	@Primary
	public static DataSource getDataSource() {
		// 数据源配置
		datasource = new DruidDataSource();
		datasource.setUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=true");
		datasource.setUsername("root");
		datasource.setPassword("root");   //这里可以做加密处理
		datasource.setDriverClassName("com.mysql.jdbc.Driver");

		// 连接池配置
		datasource.setInitialSize(5); // 初始化连接大小
		datasource.setMinIdle(5); // 最小连接池数量
		datasource.setMaxActive(20); // 最大连接池数量
		datasource.setMaxWait(60000); // 获取连接时最大等待时间，单位毫秒
		datasource.setTimeBetweenEvictionRunsMillis(6000); // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
		datasource.setMinEvictableIdleTimeMillis(300000); // 配置一个连接在池中最小生存的时间，单位是毫秒
		datasource.setValidationQuery("SELECT 1 FROM DUAL"); // 测试连接
		datasource.setTestWhileIdle(true); // 申请连接的时候检测，建议配置为true，不影响性能，并且保证安全性
		datasource.setTestOnBorrow(false); // 获取连接时执行检测，建议关闭，影响性能
		datasource.setTestOnReturn(false); // 归还连接时执行检测，建议关闭，影响性能
		datasource.setPoolPreparedStatements(false); // 是否开启PSCache，PSCache对支持游标的数据库性能提升巨大，oracle建议开启，mysql下建议关闭
		datasource.setMaxPoolPreparedStatementPerConnectionSize(20); // 开启poolPreparedStatements后生效
		try {
			datasource.setFilters("stat,wall,slf4j"); // 配置扩展插件，常用的插件有=>stat:监控统计  log4j:日志  wall:防御sql注入
		} catch (SQLException e) {
			e.printStackTrace(); // 扩展插件异常处理
		}
		datasource.setConnectionProperties("druid.stat.mergeSql=true;druid.stat.slowSqlMillis=5000"); // 通过connectProperties属性来打开mergeSql功能;慢SQL记录
		return datasource;
	}

	// 以下是Spring中的一些配置Bean
//	@Bean
//	public ServletRegistrationBean statViewServlet(){
//		ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean(new StatViewServlet(),"/druid/*");
//		servletRegistrationBean.addInitParameter("allow","127.0.0.1");  //设置ip白名单
//		servletRegistrationBean.addInitParameter("deny","192.168.10.103");//设置ip黑名单，优先级高于白名单
//		//设置控制台管理用户
//		servletRegistrationBean.addInitParameter("loginUsername","admin");
//		servletRegistrationBean.addInitParameter("loginPassword","admin");
//		//是否可以重置数据
//		servletRegistrationBean.addInitParameter("resetEnable","false");
//		return servletRegistrationBean;
//	}
//
//	@Bean(name = "druidStatFilter")
//	public FilterRegistrationBean statFilter(){
//		//创建过滤器
//		FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean(new WebStatFilter());
//		//设置过滤器过滤路径
//		filterRegistrationBean.addUrlPatterns("/*");
//		//忽略过滤的形式
//		filterRegistrationBean.addInitParameter("exclusions","*.js,*.gif,*.jpg,*.png,*.css,*.ico,/druid/*");
//		return filterRegistrationBean;
//	}
}
