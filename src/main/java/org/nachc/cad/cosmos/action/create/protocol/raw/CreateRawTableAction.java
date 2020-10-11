package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableAction {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
		log.info("Creating raw_table record");
		RawTableDvo dvo = new RawTableDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setRawTableSchema(params.getRawTableSchemaName());
		dvo.setRawTableName(params.getRawTableName());
		Dao.insert(dvo, conn);
		params.setRawTableDvo(dvo);
		log.info("Creating raw_table record");
	}

}
