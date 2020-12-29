package org.nachc.cad.cosmos.action.create.protocol.raw.mysql;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableAction {

	public static void execute(RawDataFileUploadParams params, Connection conn, boolean overwrite) {
		log.info("Creating raw_table record");
		RawTableDvo dvo = new RawTableDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setRawTableSchema(params.getRawTableSchemaName());
		dvo.setRawTableName(params.getRawTableName());
		dvo.setRawTableGroup(params.getRawTableGroupDvo().getGuid());
		dvo.setProject(params.getProjCode());
		checkOrgCode(conn);
		if(overwrite == true) {
			remove(dvo, conn);
		}
		Dao.insert(dvo, conn);
		params.setRawTableDvo(dvo);
		log.info("Creating raw_table record");
	}
	
	private static void remove(RawTableDvo dvo, Connection conn) {
		String[] keys = {"raw_table_schema","raw_table_name"};
		String[] vals = {dvo.getRawTableSchema(), dvo.getRawTableName()};
		RawTableDvo foundDvo = Dao.find(dvo, keys, vals, conn);
		if(foundDvo != null) {
			removeTableCols(dvo, conn);
			Dao.delete(foundDvo, conn);
		}
	}

	private static void removeTableCols(RawTableDvo dvo, Connection conn) {
		String sqlString = "delete from raw_table_col where raw_table = ?";
		String rawTable = dvo.getGuid();
		Database.update(sqlString, rawTable, conn);
	}
	
	private static void checkOrgCode(Connection conn) {
		Data data = Database.query("select * from org_code order by code", conn);
		log.info("Got " + data.size() + " orgs");
		for(Row row : data) {
			log.info("\t" + row.get("code"));
		}
	}
	
}
