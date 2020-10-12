package org.nachc.cad.cosmos.action.create.protocol.raw.mysql;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

public class CreateRawTableGroupRecordAction {

	public static RawTableGroupDvo execute(RawDataFileUploadParams params, Connection conn) {
		RawTableGroupDvo dvo = new RawTableGroupDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setCode(params.getRawTableGroupCode());
		dvo.setName(params.getRawTableGroupName());
		dvo.setDescription(params.getRawTableGroupDescription());
		dvo.setGroupTableSchema(params.getGroupTableSchemaName());
		dvo.setGroupTableName(params.getDataGroupName());
		dvo.setRawTableSchema(params.getRawTableSchemaName());
		Dao.insert(dvo, conn);
		params.setRawTableGroupDvo(dvo);
		return dvo;
	}

}
