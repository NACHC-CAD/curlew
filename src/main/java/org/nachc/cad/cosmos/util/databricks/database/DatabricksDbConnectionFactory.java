package org.nachc.cad.cosmos.util.databricks.database;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksDbConnectionFactory {

	public static Connection getConnection() {
		String url = DatabricksParams.getJdbcUrl();
		String token = DatabricksAuthUtil.getToken();
		log.info("URL: " + url);
		Connection conn = DatabricksDbUtil.getConnection(url, token);
		DatabricksDbUtil.initParsePolicy(conn);
		log.info("Got connection");
		return conn;
	}

}
