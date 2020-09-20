package org.nachc.cad.cosmos.util.databricks.database;

import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.file.DatabricksFileUtil;

public class DatabricksFileUtilFactory {

	public static DatabricksFileUtil get() {
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		return util;
	}

}
