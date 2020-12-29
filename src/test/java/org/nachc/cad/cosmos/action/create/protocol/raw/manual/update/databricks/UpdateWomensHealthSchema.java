package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.databricks;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateWomensHealthSchema {

	public static final String FILE = "/org/nachc/cad/cosmos/databricks/update/projects/womenshealth/init/womens-health-tables-001.sql";
	
	public static void main(String[] args) {
		log.info("Starting process..");
		log.info("Getting connection");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Running Script");
		DatabricksDbUtil.createDatabase("womens_health", conn);
		String script = FileUtil.getAsString(FILE);
		Database.executeSqlScript(script, conn);
		log.info("Done.");
	}
	
}
