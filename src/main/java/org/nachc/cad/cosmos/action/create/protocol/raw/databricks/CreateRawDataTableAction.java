package org.nachc.cad.cosmos.action.create.protocol.raw.databricks;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.RawTableProxy;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawDataTableAction {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
		String schemaName = params.getRawTableDvo().getSchemaName();
		String tableName = params.getRawTableName();
		DatabricksDbUtil.dropTable(schemaName, tableName, conn);
		// String sqlString = RawTableProxy.getDatabricksCreateTableSqlString(dvo, fileDvo, cols);
		String sqlString = RawTableProxy.getDatabricksCreateTableSqlString(params.getRawTableDvo(), params.getRawTableFileDvo(), params.getRawTableColList(), params.getDelimiter());
		log.info("Got sqlString: \n\n" + sqlString + "\n\n");
		log.info("Doing update...");
		Database.update(sqlString, conn);
		log.info("Table created");
	}
	
}
