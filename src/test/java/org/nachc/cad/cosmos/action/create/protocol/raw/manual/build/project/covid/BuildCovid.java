package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.delete.DeleteCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize.CreateCovidGroupTables;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize.CreateCovidSchemas;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210121_Covid_Loinc;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210122_Covid_CHCN;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210207_Covid_AC;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210207_Covid_HCN;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildCovid {

	//
	// main method (see exec method below for implementation)
	//

	public static void main(String[] args) {
		log.info("Starting main...");
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("executing...");
			exec(conns);
			log.info("Committing");
			conns.commit();
			log.info("Done with commit");
		} finally {
			log.info("Closing connections...");
			conns.close();
			log.info("Done with close");
		}
		log.info("Done.");
	}

	/**
	 * 
	 * This method will delete and rebuild the COVID-19 projects: - covid -
	 * covid_meta - covid_loinc
	 * 
	 */

	public static void exec(CosmosConnections conns) {
		DeleteCovidProject.exec(conns);
		conns.commit();
		CreateCovidProject.exec(conns);
		conns.commit();
		Update20210207_Covid_HCN.exec(conns);
		conns.commit();
		Update20210121_Covid_Loinc.exec(conns);
		conns.commit();
		Update20210122_Covid_CHCN.exec(conns);
		conns.commit();
		Update20210207_Covid_AC.exec(conns);
		conns.commit();
		CreateCovidGroupTables.exec(conns);
		conns.commit();
		CreateCovidSchemas.exec(conns);
		conns.commit();
	}

}
