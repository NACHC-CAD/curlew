package org.nachc.cad.cosmos.action.create.project;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateCleanedTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateCleanedTablesAction {

	public static void exec(String dataGroupName, String dataGroupAbr, String dataLot, RawDataFileUploadParams params, CosmosConnections conns) {
		log.info("Looking for existing raw table group...");
		updateParams(dataGroupName, dataGroupAbr, dataLot, params, conns);
		String rootDir = params.getLocalHostFileAbsLocation();
		log.info("Uploading files from: " + rootDir);
		List<File> files = getFiles(rootDir);
		log.info("Got " + files.size() + " files.");
		for (File file : files) {
			log.info("File: " + FileUtil.getCanonicalPath(file));
			updateParamsWithFileInfo(params, file, conns.getMySqlConnection());
			CreateCleanedTableAction.exec(params.getRawTableFileDvo().getRawTable(), conns);
		}
		log.info("Done uploading files.");
	}

	private static RawDataFileUploadParams updateParams(String dataGroupName, String dataGroupAbr, String dataLot, RawDataFileUploadParams params, CosmosConnections conns) {
		params.setDatabricksFileLocation(params.getDatabricksFilePath() + dataGroupAbr);
		params.setDataGroupName(dataGroupName);
		params.setDataGroupAbr(dataGroupAbr);
		params.setDataLot(dataLot);
		params.setDatabricksFileLocation(params.getDatabricksFileRoot() + params.getProjCode() + "/" + dataGroupAbr);
		String localHostFileAbsLocation = params.getLocalHostFileAbsLocation() + dataGroupAbr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		String code = params.getRawTableGroupCode();
		log.info("Getting raw_table_group for: " + code);
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, conns.getMySqlConnection());
		if(rawTableGroupDvo == null) {
			// if the raw table group does not exist create it
			CreateRawTableGroupAction.exec(params, conns);
			rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, conns.getMySqlConnection());
		}
		params.setRawTableGroupDvo(rawTableGroupDvo);
		return params;
	}
	
	private static List<File> getFiles(String rootDirName) {
		File rootDir = new File(rootDirName);
		List<File> rtn = FileUtil.listFiles(rootDir, "**/*");
		return rtn;
	}

	private static void updateParamsWithFileInfo(RawDataFileUploadParams params, File file, Connection conn) {
		params.setFileName(file.getName());
		params.setFile(file);
		params.setOrgCode(file.getParentFile().getName());
		if (file.getName().toLowerCase().endsWith(".txt")) {
			params.setDelimiter('|');
		} else {
			params.setDelimiter(',');
		}
		String[] keys = {"file_location", "file_name"};
		String[] vals = {params.getDatabricksFileLocation(), params.getFileName()};
		RawTableFileDvo dvo = Dao.find(new RawTableFileDvo(), keys, vals, conn);
		params.setRawTableFileDvo(dvo);
	}

}
