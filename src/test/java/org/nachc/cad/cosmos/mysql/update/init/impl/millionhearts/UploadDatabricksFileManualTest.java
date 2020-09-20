package org.nachc.cad.cosmos.mysql.update.init.impl.millionhearts;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentDefDvo;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.BlockDefProxy;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDatabricksFileManualTest {

	@Test
	public void shouldUploadFile() {
		log.info("Uploading Databricks files");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		String guid = BlockDefProxy.getGuidForCode("MILLION_HEARTS_CSV", conn);
		DocumentDefDvo docDefDvo = getDocDef(guid, "DEMO", conn);
		log.info("Got dvo: " + docDefDvo.getDatabricksDir());
		log.info("Done.");
	}
	
	private DocumentDefDvo getDocDef(String guid, String dataGroup, Connection conn) {
		DocumentDefDvo dvo = Dao.find(new DocumentDefDvo(), new String[]{"block_def", "data_group"}, new String[]{guid, dataGroup}, conn);
		return dvo;
	}
	
}
