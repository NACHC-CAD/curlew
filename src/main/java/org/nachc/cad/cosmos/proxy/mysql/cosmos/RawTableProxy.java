package org.nachc.cad.cosmos.proxy.mysql.cosmos;

import java.util.List;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;

public class RawTableProxy {

	public static String getDatabricksCreateTableSqlString(RawTableDvo dvo, RawTableFileDvo fileDvo, List<RawTableColDvo> cols) {
		return getDatabricksCreateTableSqlString(dvo, fileDvo, cols, null);
	}

	public static String getDatabricksCreateTableSqlString(RawTableDvo dvo, RawTableFileDvo fileDvo, List<RawTableColDvo> cols, Character delimiter) {
		String sqlString = "";
		sqlString += "create table " + dvo.getRawTableSchema() + "." + dvo.getRawTableName() + " ( \n";
		for (RawTableColDvo colDvo : cols) {
			if (sqlString.endsWith(" ( \n") == false) {
				sqlString += ", \n";
			}
			sqlString += "  " + colDvo.getColName() + " string";
		}
		sqlString += " \n";
		sqlString += ") \n";
		sqlString += "using csv \n";
		sqlString += "options ( \n";
		sqlString += "  header = \"true\", \n";
		sqlString += "  inferSchema = \"false\", \n";
		if (delimiter != null) {
			String delimiterString;
			if(delimiter.equals('\t')) {
				delimiterString = "\\t";
			} else {
				delimiterString = "" + delimiter;
			}
			sqlString += "  delimiter = \"" + delimiterString + "\", \n";
		}
		sqlString += "  path = \"" + fileDvo.getFileLocation() + "/" + fileDvo.getFileName() + "\" \n";
		sqlString += ") \n";
		return sqlString;
	}

}
