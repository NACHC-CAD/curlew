package org.nachc.cad.cosmos.action.create.project;

import java.io.File;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateColumnMappingsAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadFilesAction {

	public static final String PATTERN = "\\_meta\\*.xlsx";

	/**
	 *
	 * Uploads all of the files in the dir in the parms object. Creates a new raw
	 * table group if one does not exist. Throws a runtime exception if file already
	 * exists (JEG: NEED TO CONFIRM).
	 *
	 */

	public static void exec(String dataGroupName, String dataGroupAbr, String dataLot, RawDataFileUploadParams params, CosmosConnections conns) {
		log.info("Looking for existing raw table group...");
		updateParams(dataGroupName, dataGroupAbr, dataLot, params, conns);
		String rootDir = params.getLocalHostFileAbsLocation();
		log.info("Uploading files from: " + rootDir);
		List<File> files = getFiles(rootDir);
		log.info("Got " + files.size() + " files.");
		for (File file : files) {
			log.info("File: " + FileUtil.getCanonicalPath(file));
			updateParamsWithFileInfo(params, file);
			AddRawDataFileAction.execute(params, true);
		}
		log.info("Creating mappings");
		createMappings(params.getLocalHostFileAbsLocation(), conns);
		log.info("Creating cleaned table");
		CreateCleanedTablesAction.exec(dataGroupName, dataGroupAbr, dataLot, params, conns);
		log.info("Done uploading files.");
	}

	private static RawDataFileUploadParams updateParams(String dataGroupName, String dataGroupAbr, String dataLot, RawDataFileUploadParams params, CosmosConnections conns) {
		params.setDatabricksFileLocation(params.getDatabricksFileRoot() + dataGroupAbr);
		params.setDataGroupName(dataGroupName);
		params.setDataGroupAbr(dataGroupAbr);
		params.setDataLot(dataLot);
		params.setDatabricksFileLocation(params.getDatabricksFileRoot() + params.getProjCode() + "/" + dataGroupAbr);
		if(params.isLegacy() == false) {
			String localHostFileAbsLocation = params.getLocalHostFileAbsLocation() + dataGroupAbr;
			params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		} else {
			if(params.getLocalHostFileAbsRoot() == null) {
				params.setLocalHostFileAbsRoot(params.getLocalHostFileAbsLocation());
			}
			String localHostFileAbsLocation = params.getLocalHostFileAbsRoot() + params.getDataGroupAbr();
			params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		}
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

	private static void updateParamsWithFileInfo(RawDataFileUploadParams params, File file) {
		params.setFileName(file.getName());
		params.setFile(file);
		params.setOrgCode(file.getParentFile().getName());
		if (file.getName().toLowerCase().endsWith(".txt")) {
			params.setDelimiter('|');
		} else {
			params.setDelimiter(',');
		}
	}

	public static void createMappings(String rootDirName, CosmosConnections conns) {
		log.info("Creating column aliases...");
		List<File> files = FileUtil.listFiles(new File(rootDirName), PATTERN);
		for (File file : files) {
			log.info("Processing: " + file.getName());
			CreateColumnMappingsAction.exec(file, conns);
		}
		log.info("Done creating column aliases.");
	}

}
