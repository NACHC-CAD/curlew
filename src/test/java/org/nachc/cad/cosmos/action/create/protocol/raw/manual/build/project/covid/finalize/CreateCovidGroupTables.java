package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize;

import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateCovidGroupTables {

	public static void exec(CosmosConnections conns) {
		log.info("Finalizing (creating group table)...");
		RawDataFileUploadParams params = CreateCovidProject.getParams();
		String projectCode = params.getProjCode();
		List<RawTableGroupDvo> list = Dao.findList(new RawTableGroupDvo(), "project", projectCode, conns.getMySqlConnection());
		for(RawTableGroupDvo dvo : list) {
			String code = dvo.getCode();
			log.info("Creating group table for: " + code);
			CreateGrpDataTableAction.execute(code, conns, true);
		}
		log.info("Done with finalize");
	}
	
}
