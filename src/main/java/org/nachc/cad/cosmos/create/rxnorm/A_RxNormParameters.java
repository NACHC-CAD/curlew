package org.nachc.cad.cosmos.create.rxnorm;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class A_RxNormParameters {

	public static final String CSV_DIR = "C:\\_WORKSPACES\\nachc\\rxnorm\\RxNorm_full_08032020\\rrf";

	public static final String DATABRICKS_DIR = "/FileStore/tables/prod/global/terminology/rxnorm/full/csv";

	public static final String CONSO_DIR = DATABRICKS_DIR + "/conso";

	public static final String REL_DIR = DATABRICKS_DIR + "/rel";

	public static final String SAT_DIR = DATABRICKS_DIR + "/sat";

	public static final String SCHEMA_NAME = "rxnorm";

	public static void log() {
		log.info("-------------------------------------------------------------");
		log.info("Parameters: ");
		log.info("CSV_DIR        " + CSV_DIR);
		log.info("DATABRICKS_DIR " + DATABRICKS_DIR);
		log.info("SCHEMA_NAME    " + SCHEMA_NAME);
		log.info("CONSO_DIR      " + CONSO_DIR);
		log.info("REL_DIR        " + REL_DIR);
		log.info("SAT_DIR        " + SAT_DIR);
		log.info("End parameters. ");
		log.info("-------------------------------------------------------------");
	}

}
