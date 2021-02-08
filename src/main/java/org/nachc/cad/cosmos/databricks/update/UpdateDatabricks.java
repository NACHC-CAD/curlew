package org.nachc.cad.cosmos.databricks.update;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Data;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateDatabricks {

	public static void main(String[] args) {
		log.info("Updating schema...");
		CosmosConnections conns = new CosmosConnections();
		log.info("Checking for existing schema...");
		if (schemaExists(conns) == false) {
			log.info("* * * CREATING DATABASE (THIS WILL ONLY HAPPEN IF THIS IS A NEW INSTANCE * * *");
			initSchema(conns.getDbConnection());
		} else {
			log.info("Schema exists, doing update");
		}
		updateSchema(conns.getDbConnection());
		log.info("Done.");
	}

	private static boolean schemaExists(CosmosConnections conns) {
		boolean rtn = DatabricksDbUtil.databaseExists("cosmos", conns.getDbConnection(), conns);
		return rtn;
	}

	private static void initSchema(Connection conn) {
		String fileName = "/org/nachc/cad/cosmos/databricks/update/sql/000-create-schema.sql";
		File file = FileUtil.getFile(fileName);
		log.info("Executing script for: " + FileUtil.getCanonicalPath(file));
		Database.executeSqlScript(file, conn);
		log.info("Done executing init script.");
	}

	private static void updateSchema(Connection conn) {
		String msg = "";
		String sqlString = "select * from cosmos.build_version order by version_number desc";
		Data data = Database.query(sqlString, conn);
		String lastFileNumberStr = data.get(0).get("versionNumber");
		String lastFileName = data.get(0).get("fileName");
		int lastFileNumber = Integer.parseInt(lastFileNumberStr);
		log.info("Last file: " + lastFileNumber + "\t" + lastFileName);
		String dirName = "/org/nachc/cad/cosmos/databricks/update/sql";
		File dir = FileUtil.getFile(dirName);
		List<File> files = FileUtil.listFiles(dir, "*.sql");
		for(File file : files) {
			log.info("Looking at file: " + file.getName());
			int fileNumber = Integer.parseInt(file.getName().substring(0,3));
			if(fileNumber > lastFileNumber) {
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
		Database.update(sqlString, new String[] {fileNumber, fileName}, conn);
		log.info("Done updateing version table.");
	}
	
}
