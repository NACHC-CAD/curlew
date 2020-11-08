package org.nachc.cad.cosmos.create.rxnorm;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Z_RxNormBuild {

	public static void main(String[] args) {
		log.info("=============================================================");
		log.info("* * * CREATING SCHEMA FOR RXNORM * * *");
		A_RxNormParameters.log();
		B_RxNormDeleteDatabricksFiles.delete();
		C_RxNormDeleteDatabaseObjects.delete();
		D_RxNormPostFilesToDatabricks.post();
		E_RxNormCreateSchema.create();
		H_RxNormCreateDatabaseObjects.create();
		log.info("* * * DONE CREATING SCHEMA FOR RXNORM * * *");
		log.info("=============================================================");
		log.info("Done.");
	}

}
