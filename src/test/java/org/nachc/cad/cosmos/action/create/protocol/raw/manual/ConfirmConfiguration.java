package org.nachc.cad.cosmos.action.create.protocol.raw.manual;

import org.nachc.cad.cosmos.util.configuration.ConfigurationUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfirmConfiguration {

	public static void main(String[] args) {
		String databricksFiles = ConfigurationUtil.getDatabricksFileStoreInstance();
		String databricksDb = ConfigurationUtil.getDatabricksSqlInstance();
		String mySql = ConfigurationUtil.getMySqlInstance();
		String msg = "\n\n* * * CONFIGURATION * * *\n";
		msg += "Databricks Files: " + databricksFiles + "\n";
		msg += "Databricks DB:    " + databricksDb + "\n";
		msg += "MySql DB:         " + mySql + "\n";
		log.info("Configuration..." + msg);
		log.info("Done.");
	}

}
