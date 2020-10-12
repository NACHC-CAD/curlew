package org.nachc.cad.cosmos.mysql.update;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.mysql.update.codegenerator.GenerateCosmosDvos;
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
		update(conn);
	}

	public static void update(Connection conn) {
		// init the schema if it does not exist
		boolean schemaExists = schemaExists(conn);
		if (schemaExists == false) {
			initSchema(conn);
		} else {
			log.info("* * * SCHEMA EXISTS, DOING UPDATE * * *");
		}
		// update the schema
		updateSchema(conn);
		GenerateCosmosDvos.generateDvos();
		// done
		log.info("Done.");
	}
	
	private static boolean schemaExists(Connection conn) {
		log.info("Looking for schema");
		String sqlString = "select * from information_schema.schemata where schema_name = 'cosmos'";
		Data data = Database.query(sqlString, conn);
		if (data.size() > 0) {
			log.info("Schema DOES exists");
			return true;
		} else {
			log.info("Schema DOES NOT exist");
			return false;
		}
	}

	private static void initSchema(Connection conn) {
		String fileName = "/org/nachc/cad/cosmos/mysql/update/sql/000-create-schema.sql";
		File file = FileUtil.getFile(fileName, true);
		log.info("Executing script for: " + FileUtil.getCanonicalPath(file));
		Database.executeSqlScript(file, conn);
		log.info("Done executing init script.");
		log.info("Initiating database tables...");
		InitMySql.init(conn);
		Database.commit(conn);
		log.info("Done initiating database tables.");
	}

	private static void updateSchema(Connection conn) {
		String msg = "";
		String sqlString = "select * from cosmos.build_version order by version_number desc";
		Data data = Database.query(sqlString, conn);
		String lastFileNumberStr = data.get(0).get("versionNumber");
		String lastFileName = data.get(0).get("fileName");
		int lastFileNumber = Integer.parseInt(lastFileNumberStr);
		log.info("Last file: " + lastFileNumber + "\t" + lastFileName);
		String dirName = "/org/nachc/cad/cosmos/mysql/update/sql";
		File dir = FileUtil.getFile(dirName, true);
		List<File> files = FileUtil.listFiles(dir, "*.sql");
		for (File file : files) {
			log.info("Looking at file: " + file.getName());
			int fileNumber = Integer.parseInt(file.getName().substring(0, 3));
			if (fileNumber > lastFileNumber) {
				Database.executeSqlScript(file, conn);
				updateVersionTable(file, conn);
				msg += "PROCESSED: \t" + file.getName() + "\n";
			} else {
				msg += "SKIPPED:   \t" + file.getName() + "\n";
			}
		}
		log.info("Update summary: \n\n" + msg);
	}

	private static void updateVersionTable(File file, Connection conn) {
		log.info("Updating version table...");
		String fileName = file.getName();
		String fileNumber = fileName.substring(0, 3);
		String sqlString = "insert into cosmos.build_version values (?,?)";
		Database.update(sqlString, new String[] { fileNumber, fileName }, conn);
		log.info("Done updateing version table.");
	}

}
