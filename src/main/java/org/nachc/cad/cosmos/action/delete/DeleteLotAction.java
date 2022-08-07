package org.nachc.cad.cosmos.action.delete;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateBaseTablesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.string.StringUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteLotAction {

	public static void deleteLot(String projectCode, String orgCode, String lot, CosmosConnections conns) {
		Data filesToDelete = getFilesToDelete(projectCode, orgCode, lot, conns);
		deleteFilesFromDatabricks(filesToDelete, conns);
		deleteRawTableFileRecordsFromMySql(projectCode, orgCode, lot, conns);
		deleteRawTableColRecordsFromMySql(filesToDelete, conns);
		deleteRawTableRecordsFromMySql(filesToDelete, conns);
		createGroupTables(projectCode, conns);
		createBaseTables(projectCode, conns);
	}

	private static Data getFilesToDelete(String projectCode, String orgCode, String lot, CosmosConnections conns) {
		String sqlString = "";
		
//		sqlString += "select distinct \n";
//		sqlString += "	file_location, \n";
//		sqlString += "  file_name, \n";
//		sqlString += "  raw_table \n";
//		sqlString += "from \n";
//		sqlString += "	raw_table_col_detail \n";
//		sqlString += "where 1=1 \n";
//		sqlString += "	and project = ? \n";
//		sqlString += "  and org_code = ? \n";
//		sqlString += "  and data_lot = ? \n";

		sqlString += "select \n";
		sqlString += "  f.file_location, \n";
		sqlString += "  f.file_name, \n";
		sqlString += "  f.raw_table \n";
		sqlString += "from \n";
		sqlString += "  raw_table_file f \n"; 
		sqlString += "where 1=1 \n";
		sqlString += "  and f.project = '" + projectCode + "' \n";
		sqlString += "  and f.org_code = '" + orgCode + "' \n";
		sqlString += "  and f.data_lot = '" + lot + "' \n";
		sqlString += "order by  \n";
		sqlString += "  f.file_name \n";


//		String[] params = { projectCode, orgCode, lot };
//		Data data = Database.query(sqlString, params, conns.getMySqlConnection());
		Data data = Database.query(sqlString, conns.getMySqlConnection());
		return data;
	}

	
	private static void deleteFilesFromDatabricks(Data filesToDelete, CosmosConnections conns) {
		log.info("Got " + filesToDelete.size() + " files to delete");
		for (Row row : filesToDelete) {
			String path = row.get("fileLocation");
			String name = row.get("fileName");
			String fileLocation = path + "/" + name;
			if (StringUtil.isEmpty(name)) {
				log.info("SKIPPIN FILE (empty file name): " + fileLocation);
				continue;
			} else {
				log.info("DELETING FILE: " + fileLocation);
				DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().delete(fileLocation);
				log.info(resp.getResponse());
				log.info("Success: " + resp.isSuccess());
				log.info("Done with delete.");
			}
		}
	}

	private static void deleteRawTableFileRecordsFromMySql(String projectCode, String orgCode, String lot, CosmosConnections conns) {
		log.info("Deleting raw_table_file records from MySql");
		String sqlString = "delete from raw_table_file where project = ? and org_code = ? and data_lot = ?";
		String [] params = {projectCode, orgCode, lot};
		int cnt = Database.update(sqlString, params, conns.getMySqlConnection());
		log.info(cnt + " RECORDS DELETED.");
	}
	
	private static void deleteRawTableColRecordsFromMySql(Data filesToDelete, CosmosConnections conns) {
		log.info("Deleting raw_table records from MySql");
		int total = 0;
		for(Row row : filesToDelete) {
			String guid = row.get("rawTable");
			String sqlString = "delete from raw_table_col where raw_table = ?";
			log.info("Deleting raw_table_record for guid: " + guid);
			int cnt = Database.update(sqlString, guid, conns.getMySqlConnection());
			log.info(cnt + " RAW_TABLE_COL RECORDS DELETED (guid=" + guid + ").");
			total = total + cnt;
		}
		log.info(total + " TOTAL RECORDS DELETED FROM RAW_TABLE_COL");
	}
	
	private static void deleteRawTableRecordsFromMySql(Data filesToDelete, CosmosConnections conns) {
		log.info("Deleting raw_table records from MySql");
		int total = 0;
		for(Row row : filesToDelete) {
			String guid = row.get("rawTable");
			String sqlString = "delete from raw_table where guid = ?";
			log.info("Deleting raw_table_record for guid: " + guid);
			int cnt = Database.update(sqlString, guid, conns.getMySqlConnection());
			log.info(cnt + " RECORDS DELETED.");
			total = total + cnt;
		}
		log.info(total + " TOTAL RECORDS DELETED FROM RAW_TABLE");
	}
	
	private static void createGroupTables(String projectCode, CosmosConnections conns) {
		String sqlString = "select code from raw_table_group where project =  ?";
		Data data = Database.query(sqlString, projectCode, conns.getMySqlConnection());
		for(Row row : data) {
			String code = row.get("code");
			log.info("Creating group table for: " + code);
			CreateGrpDataTableAction.execute(code, conns);
			log.info("Done creating group table for: " + code);
		}
	}

	private static void createBaseTables(String projectCode, CosmosConnections conns) {
		CreateBaseTablesAction.exec(projectCode, conns);
	}

}
