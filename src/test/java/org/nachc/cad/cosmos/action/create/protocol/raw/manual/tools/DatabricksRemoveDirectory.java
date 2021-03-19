package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksRemoveDirectory {

	public static void main(String[] args) {
		//String dirName = "/FileStore/tables/prod/covidcovid";
		String dirName = "/user/hive/warehouse/prj_raw_covid_demo.db/covid_he_demo_health_efficient_covid_19__demog__sdoh__cy2020_csv_cleaned";
		log.info("Removing dir: " + dirName);
		DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().rmdir(dirName);
		log.info("Success: " + resp.isSuccess());
		log.info("STATUS: " + resp.getStatusCode());
		log.info("Response: \n" + resp.getResponse());
		log.info("Done");
	}
	
}
