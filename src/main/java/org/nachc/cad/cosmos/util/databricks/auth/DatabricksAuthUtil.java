package org.nachc.cad.cosmos.util.databricks.auth;

import java.util.Properties;

import com.nach.core.util.props.PropertiesUtil;

public class DatabricksAuthUtil {

	private static final Properties PROPS = PropertiesUtil.getAsProperties("/auth/databricks-auth.properties");
	
	public String getToken() {
		String rtn = PROPS.getProperty("token");
		return rtn;
	}
	
}
