package org.nachc.cad.cosmos.util.project;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
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
		// upload files
		uploadFiles(params, conns);
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
		params.setCreatedBy(props.getProperty("user-name"));
		params.setDataGroupAbr(props.getProperty("data-group-abbr"));
		params.setDatabricksFileLocation(props.getProperty("data-lot"));
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
		log.info("Checing for project code: " + projectCode);
		ProjCodeDvo dvo = Dao.find(new ProjCodeDvo(), "code", params.getProjCode(), conns.getMySqlConnection());
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
			log.info("PROJECT_CODE FOUND: " + dvo.getCode());
		}
	}

	private static void checkForProjectRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		String projCode = params.getProjCode();
		String projName = params.getProjName();
		String projDescription = params.getProjDescription();
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
			log.info("PROJECT FOUND FOR CODE: " + dvo.getCode());
		}
	}

	//
	// UPLOAD THE FILES
	//

	private static void uploadFiles(RawDataFileUploadParams params, CosmosConnections conns) {
		File rootDir = params.getLocalDirForUpload();
		List<File> files = FileUtil.listFiles(rootDir);
		files = FileUtil.removeStartsWith(files, "_");
		String msg = "Found the following data directories: \n";
		for (File file : files) {
			msg += file.getName() + "\n";
		}
		log.info(msg);
	}

}
