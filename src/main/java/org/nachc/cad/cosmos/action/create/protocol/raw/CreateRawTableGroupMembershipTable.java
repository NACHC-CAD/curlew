package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupRawTableDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

public class CreateRawTableGroupMembershipTable {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
		RawTableGroupRawTableDvo dvo = new RawTableGroupRawTableDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setRawTableGroup(params.getRawTableGroupDvo().getGuid());
		dvo.setRawTable(params.getRawTableDvo().getGuid());
		Dao.insert(dvo, conn);
		params.setRawTableGroupRawTableDvo(dvo);
	}

}
