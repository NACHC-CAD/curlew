package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.ConfirmConfiguration;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateFlatGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateObsGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateOtherGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update99999999CreateWomensHealthSchema {

	public static final File FILE = FileUtil.getFile("/org/nachc/cad/cosmos/databricks/update/projects/womenshealth/init/womens-health-tables-001.sql", true);

	public static void main(String[] args) {
		log.info("Confirming configuration");
		ConfirmConfiguration.main(null);
		log.info("Getting connection...");
		updateGroupTables();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Executing script...");
		Database.executeSqlScript(FILE, dbConn);
		log.info("Done creating womens health schema.");
	}
	
	private static void updateGroupTables() {
		log.info("Doing updates");
		new UpdateDemoGroupTable().doUpdate();
		new UpdateDiagGroupTable().doUpdate();
		new UpdateEncGroupTable().doUpdate();
		new UpdateFlatGroupTable().doUpdate();
		new UpdateObsGroupTable().doUpdate();
		new UpdateOtherGroupTable().doUpdate();
		new UpdateProcedureGroupTable().doUpdate();
		new UpdateRxGroupTable().doUpdate();
	}
	
}
