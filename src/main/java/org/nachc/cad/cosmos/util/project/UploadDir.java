package org.nachc.cad.cosmos.util.project;

import java.io.File;
import java.util.Properties;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.dao.Dao;

import com.nach.core.util.props.PropertiesUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDir {

	private static final String PROPS_PATH = "_meta/project.properties";

	public void uploadDir(String absPath, String userName, String dataGroupAbbr, CosmosConnections conns) {
		File dir = new File(absPath);
		uploadDir(dir, userName, dataGroupAbbr, conns);
	}

	public void uploadDir(File dir, String userName, String dataGroupAbbr, CosmosConnections conns) {
		// get the parameters
		File projectPropertiesFile = new File(dir, PROPS_PATH);
		Properties props = PropertiesUtil.getAsProperties(projectPropertiesFile);
		props.put("user-name", userName);
		props.put("data-group-abbr", dataGroupAbbr);
		RawDataFileUploadParams params = getParams(props);
		// check project records (create new project if create-new-project is true)
		checkForProjectCodeRecord(params, conns);
		checkForProjectRecord(params, conns);
	}

	//
	// implementation (all private past here
	//

	public RawDataFileUploadParams getParams(Properties props) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setProjCode(props.getProperty("project-code"));
		params.setProjName(props.getProperty("project-name"));
		params.setProjDescription(props.getProperty("project-description"));
		params.setOrgCode(props.getProperty("org"));
		params.setCreatedBy(props.getProperty("user-name"));
		params.setDataGroupAbr(props.getProperty("data-group-abbr"));
		boolean createNew = false;
		if("true".equalsIgnoreCase(props.getProperty("create-new-project"))) {
			createNew = true;
		}
		params.setCreateNewProject(createNew);
		params.setDatabricksFileRoot(DatabricksParams.getProjectFilesRoot());
		return params;
	}

	private void checkForProjectCodeRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		String projectCode = params.getProjCode();
		log.info("Checing for project code: " + projectCode);
		ProjCodeDvo dvo = Dao.find(new ProjCodeDvo(), "code", params.getProjCode(), conns.getMySqlConnection());
		if(dvo == null && params.isCreateNewProject() == true) {
			log.info("Adding new project code for: " + projectCode);
			dvo = new ProjCodeDvo();
			dvo.setCode(params.getProjCode());
			dvo.setName(params.getProjName());
			CosmosDvoUtil.init(dvo, params.getCreatedBy(), conns.getMySqlConnection());
			Dao.insert(dvo, conns.getMySqlConnection());
		} else {
			log.info("Project code exists for: " + projectCode);
		}
	}

	private void checkForProjectRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		
	}
	
}
