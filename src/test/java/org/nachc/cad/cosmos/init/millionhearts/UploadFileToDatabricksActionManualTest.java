package org.nachc.cad.cosmos.init.millionhearts;

import java.io.File;
import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.upload.UploadFileToDataBricksAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.BlockDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentDefDvo;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.BlockDefProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.PersonProxy;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadFileToDatabricksActionManualTest {

	private static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Million Hearts\\statins";
	
	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		// get connection
		log.info("Getting connection");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		// do deletes
		log.info("Doing deletes");
		delete(conn);
		// get dependencies
		log.info("Getting dependencies");
		String createdByGuid = PersonProxy.getGuidForUid("greshje", conn);
		String blockDefGuid = BlockDefProxy.getForCode("MILLION_HEARTS_CSV", conn).getGuid();
		String title = "Million Hearts Data from Mosaic";
		String description = "Million Hearts Data from Mosaic";
		// Create block
		BlockDvo blockDvo = UploadFileToDataBricksAction.createBlock(blockDefGuid, createdByGuid, "PROD", title, description, conn);
		DocumentDefDvo docDefDvo = null;
		File file = null;
		// demo
		log.info("Doing inserts for DEMO");
		file = getDemoFile();
		docDefDvo = getDocDef(blockDefGuid, "DEMO", conn);
		UploadFileToDataBricksAction.uploadFile(file, docDefDvo, blockDvo.getGuid(), createdByGuid, description, conn);
		// encounter
		log.info("Doing inserts for ENCOUNTER");
		file = getEncFile();
		docDefDvo = getDocDef(blockDefGuid, "ENC", conn);
		UploadFileToDataBricksAction.uploadFile(file, docDefDvo, blockDvo.getGuid(), createdByGuid, description, conn);
		// commit
		log.info("Doing COMMIT");
		Database.commit(conn);
		// done
		log.info("Done.");
	}

	private File getDemoFile() {
		String fileName = DIR + "\\demo\\Mosaic Demographics Statin.csv";
		File file = new File(fileName);
		return file;
	}

	private File getEncFile() {
		String fileName = DIR + "\\enc\\Mosaic Encounters Statin.csv";
		File file = new File(fileName);
		return file;
	}

	private DocumentDefDvo getDocDef(String blockDefGuid, String dataGroup, Connection conn) {
		return Dao.find(new DocumentDefDvo(), new String[] { "block_def", "data_group" }, new String[] { blockDefGuid, dataGroup }, conn);
	}

	private void delete(Connection conn) {
		log.info("Doing deletes");
		String blockDefGuid = BlockDefProxy.getGuidForCode("MILLION_HEARTS_CSV", conn);
		Database.update("delete from document where block in (select guid from block where block_def = ?)", blockDefGuid, conn);
		Database.update("delete from block where block_def = ?", blockDefGuid, conn);
		log.info("Done with deletes");
	}

}
