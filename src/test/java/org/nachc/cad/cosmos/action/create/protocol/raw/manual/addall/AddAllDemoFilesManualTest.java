package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.group.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchema;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.create.protocol.raw.util.AddRawDataFileIntegrationTestUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddAllDemoFilesManualTest {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		log.info("Getting connections");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		// do the set up (this adds data that would already exists in the system)
		RawDataFileUploadParams params = init(mySqlConn, dbConn);
		// do the upload
		List<File> files = getFiles();
		for(File file : files) {
			log.info("File: " + FileUtil.getCanonicalPath(file));
			updateParams(params, file);
			AddRawDataFileAction.execute(params, dbConn, mySqlConn);
		}
		log.info("Done.");
	}

	private static void updateParams(RawDataFileUploadParams params, File file) {
		params.setFileName(file.getName());
		params.setFile(file);
		params.setOrgCode(file.getParentFile().getName());
		if(file.getName().toLowerCase().endsWith(".txt")) {
			params.setDelimiter('|');
		} else {
			params.setDelimiter(',');
		}
	}

	private List<File> getFiles() {
		String rootDirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\demo";
		File rootDir = new File(rootDirName);
		List<File> rtn = FileUtil.listFiles(rootDir, "**/*");
		return rtn;
	}

	private RawDataFileUploadParams init(Connection mySqlConn, Connection dbConn) {
		RawDataFileUploadParams params = AddRawDataFileIntegrationTestUtil.getParams();
		cleanUpDatabricks(mySqlConn, dbConn, params);
		cleanUpMySql(mySqlConn, dbConn, params);
		return params;
	}

	private void cleanUpDatabricks(Connection mySqlConn, Connection dbConn, RawDataFileUploadParams params) {
		// clear the files off of data bricks
		DatabricksFileUtilFactory.get().rmdir("/FileStore/tables/integration-test");
		// drop the raw and grp databases in data bricks
		DatabricksDbUtil.dropDatabase(params.getGroupTableSchemaName(), dbConn);
		DatabricksDbUtil.dropDatabase(params.getRawTableSchemaName(), dbConn);
		// create the raw and grp databases in data bricks
		CreateRawDataDatabricksSchema.execute(params, dbConn);
	}
	
	private void cleanUpMySql(Connection mySqlConn, Connection dbConn, RawDataFileUploadParams params) {
		// delete the old
		DeleteRawDataGroupAction.delete(params.getRawTableGroupCode(), dbConn, mySqlConn);
		// create a new empty raw data group
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
	}

}
