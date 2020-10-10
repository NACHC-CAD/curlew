package org.nachc.cad.cosmos.action.create.protocol.raw.after;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.yaorma.database.Database;

public class CreateRawTableGroupRecordIntegrationTestHelper {

	public static CreateProtocolRawDataParams getParams() {
		CreateProtocolRawDataParams params = new CreateProtocolRawDataParams();
		params.setCreatedBy("greshje");
		params.setProtocolName("wmns_health");
		params.setProtocolNamePretty("Wmns's Health");
		params.setDataGroupName("Demographics");
		params.setDataGroupAbr("demo");
		return params;
	}

	public static void cleanUp(CreateProtocolRawDataParams params, Connection conn) {
		Database.update("delete from raw_table_group where lower(code) = 'wmns_health_demo'", conn);
	}

}
