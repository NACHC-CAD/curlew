package org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived;

import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateBaseTablesAction {

	public static void exec(RawDataFileUploadParams params, CosmosConnections conns) {
		createSchemaIfNotExists(params.getProjCode(), conns);
		List<RawTableGroupDvo> list = getBaseTables(params, conns);
		for (RawTableGroupDvo dvo : list) {
			createTable(dvo, conns);
		}
	}

	public static void exec(String projCode, CosmosConnections conns) {
		createSchemaIfNotExists(projCode, conns);
		List<RawTableGroupDvo> list = getBaseTables(projCode, conns);
		for (RawTableGroupDvo dvo : list) {
			createTable(dvo, conns);
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
		String srcTableName = dvo.getGroupTableSchema() + ".`" + dvo.getGroupTableName() + "`";
		String tableName = dvo.getProject() + ".`" + dvo.getGroupTableName() +"`";
		log.info("+ + + BASE TABLE: Creating base table for: " + dvo.getGroupTableName());
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

}
