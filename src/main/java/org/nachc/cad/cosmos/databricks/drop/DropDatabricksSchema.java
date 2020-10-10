package org.nachc.cad.cosmos.databricks.drop;

import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.json.JsonUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropDatabricksSchema {

	public static void main(String[] args) {
		log.info("* * * * * * * * * * * * * * * * * * * * * * *");
		log.info("* * *                                   * * *");
		log.info("* * *    ABOUT TO DROP COSMOS SCHEMA    * * *");
		log.info("* * *                                   * * *");
		log.info("* * * * * * * * * * * * * * * * * * * * * * *");
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		log.info("Getting connection...");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Doing drop");
		log.info("Dropping cosmos");
		DatabricksDbUtil.dropDatabase("cosmos", conn);
		log.info("Dropping rxnorm");
		DatabricksDbUtil.dropDatabase("rxnorm", conn);
		log.info("Dropping value_set");
		DatabricksDbUtil.dropDatabase("value_set", conn);
		log.info("Deleting prod files...");
		String response = util.list("/FileStore/tables");
		log.info("Got response: \n" + response);
		List<String> paths = JsonUtil.getJsonArray(response, "files");
		int cnt = 0;
		for(String pathJson : paths) {
			cnt++;
			String path = JsonUtil.getString(pathJson, "path");
			log.info("REMOVING PATH: " + cnt + " of " + paths.size() + ":\t" + path);
			util.rmdir(path);
			log.info("Done with remove");
		}
		log.info("Done.");
	}

}
