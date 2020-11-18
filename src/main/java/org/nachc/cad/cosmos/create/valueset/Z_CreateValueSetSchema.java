package org.nachc.cad.cosmos.create.valueset;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Z_CreateValueSetSchema {

	public static void main(String[] args) {
		log.info("=============================================================");
		log.info("* * * CREATING SCHEMA FOR VALUE_SET * * *");
		A_ParametersForValueSetSchema.logParameters();
		B_DeleteValueSetParsedFiles.deleteFiles();
		C_DeleteValueSetDatabricksFiles.deleteFiles();
		D_DeleteValueSetDatabaseObjects.delete();
		E_ParseValueSetFiles.parse();
		F_PostValueSetFilesToDatabricks.post();
		G_CreateValueSetSchema.create();
		H_CreateValueSetDatabaseObjects.create();
		log.info("* * * DONE CREATING SCHEMA FOR VALUE_SET * * *");
		log.info("=============================================================");
		log.info("Done.");
	}

}
