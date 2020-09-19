package org.nachc.cad.cosmos.mysql.update;

import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateMySql {

	public static void main(String[] args) {
		log.info("* * * UPDATING SCHEMA * * *");
		Connection conn = MySqlConnectionFactory.getMysqlConnection("");
		boolean schemaExists = schemaExists(conn);
		if(schemaExists == false) {
			initSchema(conn);
		} else {
			log.info("* * * SCHEMA EXISTS, DOING UPDATE * * *");
		}
		updateSchema(conn);
		log.info("Done.");
	}
	
	private static boolean schemaExists(Connection conn) {
		log.info("Looking for schema");
		String sqlString = "select * from information_schema.schemata where schema_name = 'cosmos'";
		List<Map<String, String>> data = Database.query(sqlString, conn);
		if(data.size() > 0) {
			log.info("Schema DOES exists");
			return true;
		} else {
			log.info("Schema DOES NOT exist");
			return false;
		}
	}
	
	private static void initSchema(Connection conn) {
		String fileName = "/org/nachc/cad/cosmos/mysql/update/sql/000-create-schema.sql";
		File file = FileUtil.getFile(fileName);
		log.info("Executing script for: " + FileUtil.getCanonicalPath(file));
		Database.executeSqlScript(file, conn);
		log.info("Done executing init script.");
	}

	private static void updateSchema(Connection conn) {
		log.info("* * * UPDATEING SCHEMA * * *");
		log.info("Done with update schema");
	}
}
