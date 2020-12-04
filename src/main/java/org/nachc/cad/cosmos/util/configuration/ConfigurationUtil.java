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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigurationUtil {

	public static String getDatabricksFileStoreInstance() {
		log.info("Getting files");
		List<String> files = DatabricksFileUtilFactory.get().listFileNames("/FileStore/meta/host");
		log.info("Got files");
		if (files.size() == 0) {
			return "Could not confirm instance: meta file not found.";
		}
		if (files.size() > 1) {
			return "Could not confirm instance: To many meta files.";
		}
		return files.get(0);
	}

	public static String getDatabricksSqlInstance() {
		log.info("Getting databricks connection");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Got connection");
		try {
			Data data = DatabricksDbUtil.showSchemas(conn);
			ArrayList<String> matches = new ArrayList<String>();
			log.info("Got data.");
			for (Row row : data) {
				String namespace = row.get("namespace");
				if (namespace != null && namespace.startsWith("this_is_")) {
					matches.add(namespace);
				}
			}
			log.info("Got data");
			if (matches.size() == 0) {
				return "Could not confirm instance: no matching records found.";
			}
			if (matches.size() == 0) {
				return "Could not confirm instance: no matching records found.";
			}
			if (matches.size() > 1) {
				return "Could not confirm instance: to many matching records.";
			}
			return matches.get(0);
		} finally {
			Database.close(conn);
		}
	}

	public static String getMySqlInstance() {
		log.info("Getting mysql connection");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Got connection");
		try {
			String sqlString = "select * from information_schema.schemata where lower(schema_name) like 'this_is_%'";
			Data data = Database.query(sqlString, conn);
			log.info("Got data");
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
