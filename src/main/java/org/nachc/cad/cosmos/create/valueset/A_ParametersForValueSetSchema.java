package org.nachc.cad.cosmos.create.valueset;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class A_ParametersForValueSetSchema {

	public static final String SCHEMA_NAME = "value_set";

	public static final String DATABRICKS_FILE_PATH = "/FileStore/tables/prod/global/value_set";

	public static final String DATABRICKS_META_FILE_PATH = "/FileStore/tables/prod/global/value_set_meta";

	public static final String FILE_ROOT = "C:\\_WORKSPACES\\nachc\\value-sets";

	public static final String EXCEL_FILE_ROOT = FILE_ROOT + "\\excel";

	public static final String CSV_FILE_ROOT = FILE_ROOT + "\\csv";

	public static final String META_FILE_ROOT = FILE_ROOT + "\\meta";

	public static void logParameters() {
		log.info("-------------------------------------------------------------");
		log.info("Parameters: ");
		log.info("SCHEMA_NAME         " + SCHEMA_NAME);
		log.info("DATABRICKS_FILE_PATH" + DATABRICKS_FILE_PATH);
		log.info("FILE_ROOT           " + FILE_ROOT);
		log.info("EXCEL_FILE_ROOT     " + EXCEL_FILE_ROOT);
		log.info("CSV_FILE_ROOT       " + CSV_FILE_ROOT);
		log.info("META_FILE_ROOT      " + META_FILE_ROOT);
		log.info("End parameters. ");
		log.info("-------------------------------------------------------------");
	}

}
