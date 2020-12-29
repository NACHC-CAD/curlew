package org.nachc.cad.cosmos.action.delete;

import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteSingleFileAction {

	/**
	 * 
	 * Deletes all of the artifacts in MySql and Databricks associated with a Raw
	 * Data Group.
	 * 
	 * A Raw Data Group is all of the data for a given project and data group (e.g.
	 * women's health demographics)
	 * 
	 */
	public static void delete(String rawTableSchema, String rawTableName, Connection mySqlConn, Connection dbConn) {
		log.info("* * * DOING DROP * * *");
		log.info("---------");
		log.info("Doing drop for: " + rawTableSchema + "." + rawTableName);
		log.info("---------");
		RawTableDvo rawTableDvo = getRawTableDvo(rawTableSchema, rawTableName, mySqlConn);
		if (rawTableDvo != null) {
			RawTableGroupDvo rawTableGroupDvo = getRawTableGroupDvo(rawTableDvo, mySqlConn);
			RawTableFileDvo rawTableFileDvo = getRawTableFileDvo(rawTableDvo, mySqlConn);
			log.info("Got RawTableDvo:      " + rawTableDvo.getGuid());
			log.info("Got RawTableGroupDvo: " + rawTableGroupDvo.getGuid());
			log.info("Got RawTableFileDvo:  " + rawTableFileDvo.getGuid());
			
			// * * *
			//
			// DROP THE TABLE
			// DELETE THE FILE
			// DO MYSQL DELETES
			// RE-BUILD THE GROUP TABLE
			//
			// * * *

			// databricks: drop the raw table
			String qualifiedName = rawTableDvo.getSchemaName() + "." + rawTableDvo.getRawTableName();
			log.info("Deleting table: " + qualifiedName);
			DatabricksDbUtil.dropTable(qualifiedName, dbConn);
			// databricks: delete the file
			String fileName = rawTableFileDvo.getFileLocation() + "." + rawTableFileDvo.getFileName();
			log.info("Deleting file: " + fileName);
			DatabricksFileUtilFactory.get().delete(fileName);
			// mysql: delete the column records
			int cnt = 0;
			cnt = Database.update("delete from raw_table_col where raw_table = ?", rawTableDvo.getGuid(), mySqlConn);
			log.info(cnt + " RAW_TABLE_COL records deleted");
			// mysql: delete the raw table file record
			cnt = Database.update("delete from raw_table_file where raw_table = ?", rawTableDvo.getGuid(), mySqlConn);
			log.info(cnt + " RAW_TABLE_FILE records deleted");
			// mysql: delete the raw table record
			cnt = Database.update("delete from raw_table where guid = ?", rawTableDvo.getGuid(), mySqlConn);
			log.info(cnt + " RAW_TABLE records deleted");
			// mysql: commit the changes
			log.info("Doing commit");
			Database.commit(mySqlConn);
			// databricks: update the group table
			log.info("Creating group table...");
			CreateGrpDataTableAction.execute(rawTableGroupDvo.getCode(), dbConn, mySqlConn, true);
			// done
			log.info("Done with delete");
		} else {
			log.warn("DVO NOT FOUND: No matching records found to delete.");
		}
	}

	private static RawTableDvo getRawTableDvo(String rawTableSchema, String rawTableName, Connection mySqlConn) {
		String[] keys = { "raw_table_schema", "raw_table_name" };
		String[] params = { rawTableSchema, rawTableName };
		RawTableDvo dvo = Dao.find(new RawTableDvo(), keys, params, mySqlConn);
		return dvo;
	}

	private static RawTableGroupDvo getRawTableGroupDvo(RawTableDvo rawTableDvo, Connection mySqlConn) {
		if(rawTableDvo == null) {
			return null;
		} else {
			return Dao.find(new RawTableGroupDvo(), "guid", rawTableDvo.getRawTableGroup(), mySqlConn);
		}
	}
	
	private static RawTableFileDvo getRawTableFileDvo(RawTableDvo rawTableDvo, Connection mySqlConn) {
		return Dao.find(new RawTableFileDvo(), "raw_table", rawTableDvo.getGuid(), mySqlConn);
	}

/*	

	private static List<RawTableColDvo> getRawTableColList(RawTableFileDvo rawTableFile, Connection mySqlConn) {
		List<RawTableColDvo> rtn = Dao.findList(new RawTableColDvo(), "raw_table", rawTableFile.getRawTable(), mySqlConn);
		return rtn;
	}

*/
	
}
