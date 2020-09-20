package org.nachc.cad.cosmos.action.upload;

import java.io.File;
import java.sql.Connection;
import java.util.Date;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.BlockDefDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.BlockDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentDefDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentDvo;
import org.yaorma.dao.Dao;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadFileToDataBricksAction {

	public static BlockDvo createBlock(String blockDefGuid, String createdByGuid, String status, String title, String desc, Connection conn) {
		BlockDvo dvo = new BlockDvo();
		dvo.setGuid(GuidFactory.getGuid());
		dvo.setBlockDef(blockDefGuid);
		dvo.setTitle(title);
		dvo.setDescription(desc);
		dvo.setStatus(status);
		Date now = TimeUtil.getNow();
		dvo.setCreatedBy(createdByGuid);
		dvo.setUpdatedBy(createdByGuid);
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		log.info("Creating BLOCK record");
		Dao.insert(dvo, conn);
		log.info("Created block: " + dvo.getGuid());
		log.info("Done with insert");
		return dvo;
	}
	
	public static void uploadFile(File file, DocumentDefDvo docDefDvo, String blockGuid, String createdByGuid, String description, Connection conn) {
		Date now = TimeUtil.getNow();
		DocumentDvo dvo = new DocumentDvo();
		dvo.setGuid(GuidFactory.getGuid());
		dvo.setBlock(blockGuid);
		dvo.setFileName(file.getName());
		dvo.setFileDescription(description);
		dvo.setDocumentDef(docDefDvo.getGuid());
		dvo.setDatabricksFileName(file.getName());
		dvo.setCreatedBy(createdByGuid);
		dvo.setUpdatedBy(createdByGuid);
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		log.info("Creating DOCUMENT record");
		Dao.insert(dvo, conn);
		log.info("Done with insert");
	}

}
