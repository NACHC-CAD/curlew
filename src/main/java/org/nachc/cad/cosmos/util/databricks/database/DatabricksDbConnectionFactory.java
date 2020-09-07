package org.nachc.cad.cosmos.util.databricks.database;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

public class DatabricksDbConnectionFactory {

	public static Connection getConnection() {
		String url = DatabricksParams.getJdbcUrl();
		String token = DatabricksAuthUtil.getToken();
		Connection conn = DatabricksDbUtil.getConnection(url, token);
		return conn;
	}
	
}
