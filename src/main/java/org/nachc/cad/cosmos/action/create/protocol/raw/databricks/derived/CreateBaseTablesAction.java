package org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived;

import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.web.listener.Listener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateBaseTablesAction {

	public static void exec(RawDataFileUploadParams params, CosmosConnections conns) {
		exec(params, conns, null);
	}

	
	public static void exec(RawDataFileUploadParams params, CosmosConnections conns, Listener lis) {
		createSchemaIfNotExists(params.getProjCode(), conns);
		List<RawTableGroupDvo> list = getBaseTables(params, conns);
		log(lis, "\nCreating base tables...");
		for (RawTableGroupDvo dvo : list) {
			createTable(dvo, conns, lis);
		}
	}

	public static void exec(String projCode, CosmosConnections conns) {
		exec(projCode, conns, null);
	}

	public static void exec(String projCode, CosmosConnections conns, Listener lis) {
		createSchemaIfNotExists(projCode, conns);
		List<RawTableGroupDvo> list = getBaseTables(projCode, conns);
		log(lis, "\nCreating base tables...");
		for (RawTableGroupDvo dvo : list) {
			createTable(dvo, conns, lis);
		}
	}

	private static List<RawTableGroupDvo> getBaseTables(RawDataFileUploadParams params, CosmosConnections conns) {
		List<RawTableGroupDvo> list = Dao.findList(new RawTableGroupDvo(), "project", params.getProjCode(), conns.getMySqlConnection());
		params.setRawTableGroupDvoList(list);
		return list;
	}

	private static List<RawTableGroupDvo> getBaseTables(String projCode, CosmosConnections conns) {
		List<RawTableGroupDvo> list = Dao.findList(new RawTableGroupDvo(), "project", projCode, conns.getMySqlConnection());
		return list;
	}

	private static void createSchemaIfNotExists(String projCode, CosmosConnections conns) {
		String sqlString = "create schema if not exists " + projCode;
		Database.update(sqlString, conns.getDbConnection());
	}

	private static void createTable(RawTableGroupDvo dvo, CosmosConnections conns) {
		createTable(dvo, conns, null);
	}

	
	private static void createTable(RawTableGroupDvo dvo, CosmosConnections conns, Listener lis) {
		String srcTableName = dvo.getGroupTableSchema() + ".`" + dvo.getGroupTableName() + "`";
		String tableName = dvo.getProject() + ".`" + dvo.getGroupTableName() +"`";
		log(lis, "+ + + BASE TABLE: Creating base table for: " + dvo.getGroupTableName());
		String sqlString;
		// drop the table if it exists
		sqlString = "drop table if exists " + tableName;
		log.info(sqlString);
		Database.update(sqlString, conns.getDbConnection());
		// create the table
		sqlString = "create table " + tableName + " as select * from " + srcTableName;
		log.info(sqlString);
		try {
			Database.update(sqlString, conns.getDbConnection());
		} catch(Exception exp) {
			// hacking this for now (the table may have been deleted as part of "delete lot")
			if(exp.getMessage() != null && exp.getMessage().toLowerCase().indexOf("table or view not found") > 0) {
				log.warn("\n\nCOULD NOT CREATE TABLE: " + tableName + " FROM " + srcTableName + "\n\n");
			} else {
				throw new RuntimeException(exp);
			}
		}
		log.info("Done creating table: " + tableName);
	}

	private static void log(Listener lis, String str) {
		log.info(str);
		if (lis != null) {
			lis.notify(str);
		}
	}

}
