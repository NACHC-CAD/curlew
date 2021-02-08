package org.nachc.cad.cosmos.action.delete;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.file.DatabricksFileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteRawDataGroupAction {

	/**
	 * 
	 * Deletes all of the artifacts in MySql and Databricks associated with a Raw
	 * Data Group.
	 * 
	 * A Raw Data Group is all of the data for a given project and data group (e.g.
	 * women's health demographics)
	 * 
	 */
	public static void delete(String rawTableGroupCode, CosmosConnections conns) {
		log.info("* * * DOING DROP * * *");
		log.info("Doing drop for: " + rawTableGroupCode);
		RawTableGroupDvo dvo = getRawTableGroup(rawTableGroupCode, conns.getMySqlConnection());
		if (dvo != null) {
			// databricks stuff
			log.info("Doing databricks drop for " + dvo.getGroupTableSchema());
			// DatabricksDbUtil.dropDatabase(dvo.getGroupTableSchema(), dbConn);
			log.info("Doing databricks drop for " + dvo.getRawTableSchema());
			// DatabricksDbUtil.dropDatabase(dvo.getRawTableSchema(), dbConn);
			log.info("Deleting files from: " + dvo.getFileLocation());
			DatabricksFileUtil fileUtil = DatabricksFileUtilFactory.get();
			fileUtil.rmdir(dvo.getFileLocation());
			// mysql stuff
			log.info("Doing mysql deletes...");
			Database.update("delete from raw_table_col where raw_table in (select guid from raw_table where raw_table_group = ?)", dvo.getGuid(), conns.getMySqlConnection());
			Database.update("delete from raw_table_file where raw_table in (select guid from raw_table where raw_table_group = ?)", dvo.getGuid(), conns.getMySqlConnection());
			Database.update("delete from raw_table where raw_table_group = ?", dvo.getGuid(), conns.getMySqlConnection());
			Database.update("delete from raw_table_group where guid = ?", dvo.getGuid(), conns.getMySqlConnection());
			Database.commit(conns.getMySqlConnection());
			log.info("Done with delete");
		}
	}

	private static RawTableGroupDvo getRawTableGroup(String rawTableGroupCode, Connection mySqlConn) {
		RawTableGroupDvo rtn = Dao.find(new RawTableGroupDvo(), "code", rawTableGroupCode, mySqlConn);
		return rtn;
	}

}
