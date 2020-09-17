package org.nachc.cad.cosmos.util.mysql.connection;

import java.sql.Connection;
import java.sql.DriverManager;

import org.nachc.cad.cosmos.util.mysql.params.MySqlParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlConnectionFactory {

	public static Connection getCosmosConnection() {
		return getMysqlConnection("cosmos");
	}

	public static Connection getMysqlConnection(String schema) {
		try {
			String url = MySqlParams.getUrl();
			String uid = MySqlParams.getUid();
			String pwd = MySqlParams.getPwd();
			url = url + "/" + schema;
			url = url + "?rewriteBatchedStatements=true";
			log.info("URL: " + url);
			Connection conn = DriverManager.getConnection(url, uid, pwd);
			return conn;
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}

	}
}
