package org.nachc.cad.cosmos.util.configuration;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

public class ConfigurationUtil {

	public static String getDatabricksFileStoreInstance() {
		List<String> files = DatabricksFileUtilFactory.get().listFileNames("/FileStore/tables/meta/host");
		if (files.size() == 0) {
			return "Could not confirm instance: meta file not found.";
		}
		if (files.size() > 1) {
			return "Could not confirm instance: To many meta files.";
		}
		return files.get(0);
	}

	public static String getDatabricksSqlInstance() {
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		try {
			Data data = DatabricksDbUtil.showSchemas(conn);
			ArrayList<String> matches = new ArrayList<String>();
			for (Row row : data) {
				String namespace = row.get("namespace");
				if (namespace != null && namespace.startsWith("this_is_")) {
					matches.add(namespace);
				}
			}
			return "Could not confirm instance: no matching records found.";
		} finally {
			Database.close(conn);
		}
	}

	public static String getMySqlInstance() {
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		try {
			String sqlString = "select * from information_schema.schemata where lower(schema_name) like 'this_is_%'";
			Data data = Database.query(sqlString, conn);
			if (data.size() == 0) {
				return "Could not confirm instance: no matching records found.";
			}
			if (data.size() == 0) {
				return "Could not confirm instance: no matching records found.";
			}
			if (data.size() > 1) {
				return "Could not confirm instance: to many matching records.";
			}
			return data.get(0).get("schemaName");
		} finally {
			Database.close(conn);
		}
	}

}
