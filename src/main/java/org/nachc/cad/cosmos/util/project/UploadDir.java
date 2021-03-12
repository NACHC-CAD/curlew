package org.nachc.cad.cosmos.util.project;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.nachc.cad.cosmos.action.create.project.CreateRawTableGroupAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateColumnMappingsAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.OrgCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.dao.Dao;

import com.nach.core.util.file.FileUtil;
import com.nach.core.util.props.PropertiesUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDir {

	private static final String PROPS_PATH = "_meta/project.properties";

	/**
	 * 
	 * Upload a directory of files given path as a String
	 * 
	 */

	public static void exec(String absPath, String userName, CosmosConnections conns) {
		File dir = new File(absPath);
		uploadDir(dir, userName, conns);
	}

	/**
	 * 
	 * Upload a directory of files given path as a String
	 * 
	 */

	public static void uploadDir(File dir, String userName, CosmosConnections conns) {
		// get the parameters
		File projectPropertiesFile = new File(dir, PROPS_PATH);
		Properties props = PropertiesUtil.getAsProperties(projectPropertiesFile);
		props.put("user-name", userName);
		RawDataFileUploadParams params = getParams(props);
		params.setLocalHostFileAbsLocation(FileUtil.getCanonicalPath(dir));
		params.setLocalDirForUpload(dir);
		// check project records (create new project if create-new-project is true)
		checkForProjectCodeRecord(params, conns);
		checkForProjectRecord(params, conns);
		checkForOrgRecord(params, conns);
		// upload files
		uploadDataDirFiles(params, conns);
	}

	// ------------------------------------------------------------------------
	//
	// implementation (all private past here)
	//
	// ------------------------------------------------------------------------

	//
	// Get the parameters (parameters are parsed from
	// - A file in the source files (./_meta/project.properties)
	// - The username (passed in as a string)
	// - The data group (demo, enc, rx, etc.) are based on the file structure
	//

	public static RawDataFileUploadParams getParams(Properties props) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setProjCode(props.getProperty("project-code"));
		params.setProjName(props.getProperty("project-name"));
		params.setProjDescription(props.getProperty("project-description"));
		params.setOrgCode(props.getProperty("org"));
		params.setOrgName(props.getProperty("org-name"));
		params.setCreatedBy(props.getProperty("user-name"));
		params.setDataGroupAbr(props.getProperty("data-group-abbr"));
		params.setDataLot(props.getProperty("data-lot"));
		params.setCreateNewOrg("true".equalsIgnoreCase(props.getProperty("create-new-org")));
		params.setCreateNewProject("true".equalsIgnoreCase(props.getProperty("create-new-project")));
		params.setOverwriteExistingFiles("true".equalsIgnoreCase(props.getProperty("overwrite-existing-files")));
		params.setDatabricksFileRoot(DatabricksParams.getProjectFilesRoot());
		return params;
	}

	//
	// A project needs to exist. If the create-new-project flag is set in the
	// project.properties file it is created here (project_code and project are
	// created)
	//

	private static void checkForProjectCodeRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		String projectCode = params.getProjCode();
		log.info("Checing for project_code record: " + projectCode);
		ProjCodeDvo dvo = Dao.find(new ProjCodeDvo(), "code", projectCode, conns.getMySqlConnection());
		if (dvo == null && params.isCreateNewProject() == true) {
			log.info("Adding new project code for: " + projectCode);
			dvo = new ProjCodeDvo();
			dvo.setCode(params.getProjCode());
			dvo.setName(params.getProjName());
			Dao.insert(dvo, conns.getMySqlConnection());
		} else if (dvo == null && params.isCreateNewProject() == false) {
			String msg = "";
			msg += "NO PROJECT_CODE FOUND AND CREATE PROJECT FLAG NOT SET: \n";
			msg += "  Either project code is wrong: " + projectCode + "\n";
			msg += "  Or create-new-project=true needs to be added to ./_meta/project.properties file";
			log.error("ERROR: \n" + msg);
			throw new RuntimeException(msg);
		} else {
			log.info("PROJECT_CODE FOUND: " + projectCode);
		}
	}

	//
	// check for project record
	//
	
	private static void checkForProjectRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		String projCode = params.getProjCode();
		String projName = params.getProjName();
		String projDescription = params.getProjDescription();
		log.info("Checing for project record: " + projCode);
		ProjectDvo dvo = Dao.find(new ProjectDvo(), "code", projCode, conns.getMySqlConnection());
		if (dvo == null && params.isCreateNewProject() == true) {
			log.info("Adding new project code for: " + projCode);
			dvo = new ProjectDvo();
			dvo.setCode(projCode);
			dvo.setName(projName);
			dvo.setDescription(projDescription);
			CosmosDvoUtil.init(dvo, params.getCreatedBy(), conns.getMySqlConnection());
			Dao.insert(dvo, conns.getMySqlConnection());
		} else if (dvo == null && params.isCreateNewProject() == false) {
			String msg = "";
			msg += "NO PROJECT FOUND AND CREATE PROJECT FLAG NOT SET: \n";
			msg += "  Either project code is wrong: " + projCode + "\n";
			msg += "  Or create-new-project=true needs to be added to ./_meta/project.properties file";
			log.error("ERROR: \n" + msg);
			throw new RuntimeException(msg);
		} else {
			log.info("PROJECT FOUND FOR CODE: " + projCode);
		}
	}

	//
	// check for org record
	//
	
	private static void checkForOrgRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		String orgCode = params.getOrgCode();
		OrgCodeDvo dvo = Dao.find(new OrgCodeDvo(), "code", orgCode, conns.getMySqlConnection());
		if (dvo == null && params.isCreateNewProject() == true) {
			log.info("Adding new ORG code for: " + orgCode);
			dvo = new OrgCodeDvo();
			dvo.setCode(orgCode);
			dvo.setName(params.getOrgName());
			Dao.insert(dvo, conns.getMySqlConnection());
		} else if (dvo == null && params.isCreateNewOrg() == false) {
			String msg = "";
			msg += "NO ORG FOUND AND CREATE ORG FLAG NOT SET: \n";
			msg += "  Either org code is wrong: " + orgCode + "\n";
			msg += "  Or create-new-org=true needs to be added to ./_meta/project.properties file";
			log.error("ERROR: \n" + msg);
			throw new RuntimeException(msg);
		} else {
			log.info("ORG FOUND FOR CODE: " + dvo.getCode());
		}
	}
	
	//
	// upload each directory
	//

	private static void uploadDataDirFiles(RawDataFileUploadParams params, CosmosConnections conns) {
		File rootDir = params.getLocalDirForUpload();
		File mappingFile = getMappingFile(rootDir);
		params.setMappingFile(mappingFile);
		List<File> dataDirs = FileUtil.listFiles(rootDir);
		dataDirs = FileUtil.removeStartsWith(dataDirs, "_");
		String msg = "";
		msg += "* * * DIRECTORIES FOR UPLOAD * * *\n";
		for (File dir : dataDirs) {
			msg += dir.getName() + "\n";
		}
		msg +=  "* * * END DIRS * * *";
		log.info("Uploading the following dir: \n" + msg);
		for (File dir : dataDirs) {
			uploadDataFiles(params, dir, conns);
		}
		log.info("Successfully uploaded files from the following dirs: \n" + msg);
	}

	//
	// upload each file
	//

	private static void uploadDataFiles(RawDataFileUploadParams params, File dir, CosmosConnections conns) {
		log.info("UPLOADING FILES: " + dir);
		String dataGroupAbbr = dir.getName();
		params.setDataGroupAbr(dataGroupAbbr);
		params.setDataGroupName(dataGroupAbbr);
		String code = params.getRawTableGroupCode();
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, conns.getMySqlConnection());
		if(rawTableGroupDvo == null) {
			// if the raw table group does not exist create it
			CreateRawTableGroupAction.exec(params, conns);
			rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, conns.getMySqlConnection());
		}
		params.setRawTableGroupDvo(rawTableGroupDvo);
		File mappingFile = params.getMappingFile();
		List<File> files = FileUtil.listFiles(dir);
		for (File file : files) {
			log.info("UPLOADING FILE: " + file.getName());
			updateParamsWithFileInfo(params, file);
			AddRawDataFileAction.execute(params, conns);
			if(mappingFile != null) {
				log.info("* * * MAPPING FILE FOUND, DOING MAPPINGS * * *");
				CreateColumnMappingsAction.exec(mappingFile, file, conns);
			} else {
				log.info("* * * MAPPING FILE NOT FOUND, MAPPINGS SKIPPED * * *");
			}
		}
	}

	//
	// update parameters for each data file
	//
	
	private static void updateParamsWithFileInfo(RawDataFileUploadParams params, File file) {
		params.setFileName(file.getName());
		params.setFile(file);
		params.setDatabricksFileLocation(params.getDatabricksFileRoot() + params.getProjCode() + "/" + params.getDataGroupAbr());
		if (file.getName().toLowerCase().endsWith(".txt")) {
			params.setDelimiter('|');
		} else {
			params.setDelimiter(',');
		}
	}

	//
	// get the mapping file
	//
	
	private static File getMappingFile(File rootDir) {
		List<File> files = FileUtil.listFiles(rootDir, "_meta/*.xlsx");
		if(files.size() > 0) {
			return files.get(0);
		} else {
			return null;
		}
	}
	
}
