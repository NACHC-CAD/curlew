package org.nachc.cad.cosmos.action.create.protocol.raw.mysql;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableAction {

	public static void execute(RawDataFileUploadParams params, Connection conn) {
		log.info("Creating raw_table record");
		RawTableDvo dvo = new RawTableDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setRawTableSchema(params.getRawTableSchemaName());
		dvo.setRawTableName(params.getRawTableName());
		dvo.setRawTableGroup(params.getRawTableGroupDvo().getGuid());
		dvo.setProject(params.getProjCode());
		Dao.insert(dvo, conn);
		params.setRawTableDvo(dvo);
		log.info("Creating raw_table record");
	}

}
