package org.nachc.cad.cosmos.mysql.update;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.mysql.update.init.InitMySql;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Data;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateMySql {

	public static void main(String[] args) {
		log.info("* * * UPDATING SCHEMA * * *");
		// get the connection
		Connection conn = MySqlConnectionFactory.getMysqlConnection("");
		// init the schema if it does not exist
		boolean schemaExists = schemaExists(conn);
		if(schemaExists == false) {
			initSchema(conn);
		} else {
			log.info("* * * SCHEMA EXISTS, DOING UPDATE * * *");
		}
		// update the schema
		updateSchema(conn);
		// done
		log.info("Done.");
	}
	
	private static boolean schemaExists(Connection conn) {
		log.info("Looking for schema");
		String sqlString = "select * from information_schema.schemata where schema_name = 'cosmos'";
		Data data = Database.query(sqlString, conn);
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
		log.info("Initiating database tables...");
		InitMySql.init(conn);
		Database.commit(conn);
		log.info("Done initiating database tables.");
	}

	private static void updateSchema(Connection conn) {
		log.info("* * * UPDATEING SCHEMA * * *");
		log.info("Done with update schema");
	}
}
