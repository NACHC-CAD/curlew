package org.nachc.cad.cosmos.action.create.project;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchemaAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateProjectAction {

	/**
	 * 
	 * This method creates the new table entries for a new project.
	 * 
	 * This method will delete all existing entities for the given project from both
	 * Databricks and MySql.
	 * 
	 */

	public static void exec(RawDataFileUploadParams params, CosmosConnections conns) {
		createProjCodeRecord(params, conns);
		createProjectRecord(params, conns);
	}

	//
	// create project records
	//
	
	private static void createProjCodeRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		ProjCodeDvo dvo = new ProjCodeDvo();
		dvo.setCode(params.getProjCode());
		dvo.setName(params.getProjName());
		Dao.insert(dvo, conns.getMySqlConnection());
	}

	private static void createProjectRecord(RawDataFileUploadParams params, CosmosConnections conns) {
		Connection mySqlConn = conns.getMySqlConnection();
		String code = params.getProjCode();
		String name = params.getProjName();
		String desc = params.getProjDescription();
		ProjectDvo dvo = new ProjectDvo();
		CosmosDvoUtil.init(dvo, "greshje", mySqlConn);
		dvo.setCode(code);
		dvo.setDescription(desc);
		dvo.setName(name);
		log.info("Doing insert");
		Dao.insert(dvo, mySqlConn);
	}

}
