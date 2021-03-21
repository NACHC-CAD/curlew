package org.nachc.cad.cosmos.action.create.protocol.raw.mysql;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableFileAction {

	public static void execute(RawDataFileUploadParams params, Connection conn, boolean isOverwrite) {
		log.info("Creating raw_table_file record");
		RawTableFileDvo dvo = new RawTableFileDvo();
		CosmosDvoUtil.init(dvo, params.getCreatedBy(), conn);
		dvo.setFileLocation(params.getDatabricksFilePathWithoutFileNameAndWithoutTrailingSeparator());
		dvo.setFileName(params.getFile().getName());
		dvo.setFileSize(FileUtil.getSize(params.getFile()));
		dvo.setFileSizeUnits("B");
		dvo.setRawTable(params.getRawTableDvo().getGuid());
		dvo.setOrgCode(params.getOrgCode());
		dvo.setProject(params.getProjCode());
		dvo.setDataLot(params.getDataLot());
		if(isOverwrite == true) {
			remove(dvo, conn);
		}
		Dao.insert(dvo, conn);
		params.setRawTableFileDvo(dvo);
		log.info("Done creating raw_table_file record");
	}
	
	private static void remove(RawTableFileDvo dvo, Connection conn) {
		RawTableFileDvo foundDvo = Dao.find(dvo, "raw_table", dvo.getRawTable(), conn);
		if(foundDvo != null) {
			Database.update("delete from raw_table_file where raw_table = ?", dvo.getRawTable(), conn);
		}
	}
	
}
