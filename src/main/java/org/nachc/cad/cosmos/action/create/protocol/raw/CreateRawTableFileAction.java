package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableFileAction {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
		log.info("Creating raw_table_file record");
		RawTableFileDvo dvo = new RawTableFileDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setFileLocation(params.getDatabricksFileLocation());
		dvo.setFileName(params.getFile().getName());
		dvo.setFileSize(FileUtil.getSize(params.getFile()));
		dvo.setFileSizeUnits("B");
		dvo.setRawTable(params.getRawTableDvo().getGuid());
		dvo.setProjCode(params.getProjCode());
		dvo.setOrgCode(params.getOrgCode());
		Dao.insert(dvo, conn);
		params.setRawTableFileDvo(dvo);
		log.info("Done creating raw_table_file record");
	}
	
}
