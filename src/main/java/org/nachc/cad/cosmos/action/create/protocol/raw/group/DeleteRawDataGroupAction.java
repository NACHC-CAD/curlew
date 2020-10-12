package org.nachc.cad.cosmos.action.create.protocol.raw.group;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;

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

	public static void execute(String rawTableGroupCode, Connection dbConn, Connection mySqlConn) {
		RawTableGroupDvo dvo = getRawTableGroup(rawTableGroupCode, mySqlConn);

		DatabricksDbUtil.dropDatabase(dvo.getGroupTableSchema(), dbConn);
		DatabricksDbUtil.dropDatabase(dvo.getRawTableSchema(), dbConn);
		DatabricksFileUtil fileUtil = DatabricksFileUtilFactory.get();
		
		
		Database.update("delete from raw_table_group_raw_table where raw_table_group = ?", dvo.getGuid(), mySqlConn);
		Database.update("delete from raw_table_col where raw_table in (select raw_table from raw_table_group_raw_table where raw_table_group = ?)", dvo.getGuid(), mySqlConn);
		Database.update("delete from raw_table_file where raw_table in (select raw_table from raw_table_group_raw_table where raw_table_group = ?)", dvo.getGuid(), mySqlConn);
		Database.update("delete from raw_table where guid in (select raw_table from raw_table_group_raw_table where raw_table_group = ?)", dvo.getGuid(), mySqlConn);
		Database.update("delete from raw_table_group where guid = ?", dvo.getGuid(), mySqlConn);
	}

	private static RawTableGroupDvo getRawTableGroup(String rawTableGroupCode, Connection mySqlConn) {
		RawTableGroupDvo rtn = Dao.find(new RawTableGroupDvo(), "code", rawTableGroupCode, mySqlConn);
		return rtn;
	}

}
