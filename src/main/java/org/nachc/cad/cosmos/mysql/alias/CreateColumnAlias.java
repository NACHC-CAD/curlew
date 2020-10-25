package org.nachc.cad.cosmos.mysql.alias;

import java.sql.Connection;

import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateColumnAlias {

	public static void execute(String groupCode, String tableSchema, String tableName, String colName, String colAlias, Connection conn) {
		String[] params = { colAlias, groupCode, tableSchema, tableName, colName };
		String sqlString = getSqlString();
		int cnt = Database.update(sqlString, params, conn);
		log.info("Rows updated: " + cnt);
		if(cnt > 1) {
			Database.rollback(conn);
			throw new RuntimeException("Attempt to update more than one row not allowed here: " + cnt);
		}
	}

	private static String getSqlString() {
		String sqlString = "";
		sqlString += "update \n";
		sqlString += "	raw_table_col col \n";
		sqlString += "	join raw_table tbl on col.raw_table = tbl.guid \n";
		sqlString += "    join raw_table_group grp on tbl.raw_table_group = grp.guid \n";
		sqlString += "set \n";
		sqlString += "	col_alias = ? \n";
		sqlString += " where 1=1 \n";
		sqlString += "	and grp.code = ? \n";
		sqlString += "  and tbl.raw_table_schema = ? \n";
		sqlString += "  and tbl.raw_table_name = ? \n";
		sqlString += "  and col.col_name = ? \n";
		return sqlString;
	}

}
