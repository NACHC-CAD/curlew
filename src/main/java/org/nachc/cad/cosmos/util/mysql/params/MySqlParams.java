package org.nachc.cad.cosmos.util.mysql.params;

import java.util.Properties;

import com.nach.core.util.props.PropertiesUtil;

public class MySqlParams {

	private static final Properties PROPS = PropertiesUtil.getAsProperties("/auth/mysql-auth.properties");

	public static String getUrl() {
		return PROPS.getProperty("url");
	}
	
	public static String getUid() {
		return PROPS.getProperty("uid");
	}
	
	public static String getPwd() {
		return PROPS.getProperty("pwd");
	}
	
	public static String getKey() {
		return PROPS.getProperty("key");
	}
	
	public static String getServerFileRoot() {
		return PROPS.getProperty("sever-file-root");
	}
	
}
