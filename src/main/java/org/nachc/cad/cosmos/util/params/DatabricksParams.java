package org.nachc.cad.cosmos.util.params;

import java.util.Properties;

import com.nach.core.util.props.PropertiesUtil;

public class DatabricksParams {

	private static final Properties PROPS = PropertiesUtil.getAsProperties("/databricks.properties");
	
	//
	// urls
	//
	
	public static String getJdbcUrl() {
		return PROPS.getProperty("jdbc-url");
	}
	
	public static String getRestUrl() {
		return PROPS.getProperty("rest-url");
	}
	
	public static String getProjectFilesRoot() {
		return PROPS.getProperty("project-files-root");
	}
	
	//
	// value set
	//
	
	public static String getValueSetCsvPath() {
		return PROPS.getProperty("value-set-csv-path");
	}
	
	public static String getValueSetExcelPath() {
		return PROPS.getProperty("value-set-excel-path");
	}
	
	public static String getValueSetExcelDir() {
		return PROPS.getProperty("value-set-excel-dir");
	}
	
	//
	// rxnorm
	//
	
	public static String getRxNormRrfDir() {
		return PROPS.getProperty("rxnorm-full-file-root");
	}

	public static String getRxNormDatabricksDir() {
		return PROPS.getProperty("rxnorm-full-csv-path");
	}

}
