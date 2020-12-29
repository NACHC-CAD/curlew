package org.nachc.cad.cosmos.action.refresh;

import java.sql.Connection;

import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RefreshSchemaAction {

	public static void exec(String schemaName, Connection dbConn) {
		log.info("Refreshing schema");
		Data data = Database.query("show tables in " +schemaName, dbConn);
		log.info("Got " + data.size() + " tables");
		int cnt = 0;
		for(Row row : data) {
			cnt++;
			String tableName = row.get("tablename");
			log.info(cnt + " of " + data.size());
			String qualifiedTableName = schemaName + "." + tableName;
			log.info("Refreshing: " + qualifiedTableName);
			Database.update("refresh table " + qualifiedTableName, dbConn);
		}
		log.info("Done with refresh");
	}
	
}
