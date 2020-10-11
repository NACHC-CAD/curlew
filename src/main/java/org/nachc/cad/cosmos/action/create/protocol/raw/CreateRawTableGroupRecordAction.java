package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

public class CreateRawTableGroupRecordAction {

	public static RawTableGroupDvo execute(CreateProtocolRawDataParams params, Connection conn) {
		RawTableGroupDvo dvo = new RawTableGroupDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setCode((params.getProtocolName() + "_" + params.getDataGroupAbr()).toUpperCase());
		dvo.setName(params.getProtocolNamePretty() + " " + params.getDataGroupName() + " Table");
		dvo.setDescription("This table contains the raw data for the " + params.getDataGroupName());
		dvo.setGroupTableSchema(("prj_grp_" + params.getProtocolName()).toLowerCase());
		dvo.setGroupTableName(params.getDataGroupName());
		Dao.insert(dvo, conn);
		return dvo;
	}

}
