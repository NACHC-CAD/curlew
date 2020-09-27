package org.nachc.cad.cosmos.init.womenshealth;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.PersonDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.PersonProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.RawTableGroupColProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.RawTableProxy;
import org.nachc.cad.cosmos.util.column.ColumnName;
import org.nachc.cad.cosmos.util.column.ColumnNameUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.exception.DatabricksFileException;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddRawDataFileManualTest {

	@Test
	public void shouldAddRawDataFile() {
		log.info("Starting test...");
		log.info("DOING rmdir (COMMENT THIS OUT)");
		log.info(DatabricksFileUtilFactory.get().rmdir(this.getFileLocation()).getResponse());
		log.info("Done with rmdir");
		// get the file
		File file = getOchin();
		// get the connection and the created by record
		log.info("Getting connection...");
		Connection mysqlConn = MySqlConnectionFactory.getCosmosConnection();
		// get databricks conn
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		PersonDvo createdBy = PersonProxy.getForUid("greshje", mysqlConn);
		// create the records for the incoming file
		addFile(getOchin(), createdBy, mysqlConn, dbConn);
		addFile(getAliance(), createdBy, mysqlConn, dbConn);
		addFile(getDenver(), createdBy, mysqlConn, dbConn);
		// done
		log.info("Done.");
	}

	private void addFile(File file, PersonDvo createdBy, Connection mysqlConn, Connection dbConn) {
		log.info("Creating records...");
		RawTableDvo tableDvo = createRawTableRecord(file, mysqlConn);
		RawTableFileDvo fileDvo = createRawTableFileRecord(tableDvo, file, mysqlConn);
		List<RawTableColDvo> rawCols = createRawTableColRecords(tableDvo, file, mysqlConn);
		RawTableGroupDvo groupDvo = getGroup(mysqlConn);
		List<RawTableGroupColDvo> existingCols = getExistingColumns(groupDvo, mysqlConn);
		List<RawTableGroupColDvo> allCols = addColumnsToGroup(groupDvo, rawCols, existingCols, createdBy.getGuid(), mysqlConn);
		createDatabricksRawDataTable(tableDvo, fileDvo, rawCols, dbConn);
		writeFileToDatabricks(file, fileDvo);
	}

	//
	// mysql methods
	//

	private RawTableDvo createRawTableRecord(File file, Connection conn) {
		log.info("Creating raw_table record");
		RawTableDvo dvo = new RawTableDvo();
		CosmosDvoUtil.init(dvo, "greshje", conn);
		dvo.setRawTableSchema("prj_raw_womens_health");
		String tableName = ColumnNameUtil.getCleanName(file.getName());
		dvo.setRawTableName(tableName);
		Dao.insert(dvo, conn);
		log.info("Done creating raw_table record");
		return dvo;
	}

	private RawTableFileDvo createRawTableFileRecord(RawTableDvo rawTableDvo, File file, Connection conn) {
		log.info("Creating raw_table_file record");
		RawTableFileDvo dvo = new RawTableFileDvo();
		CosmosDvoUtil.init(dvo, "greshje", conn);
		dvo.setRawTable(rawTableDvo.getGuid());
		dvo.setFileLocation(getFileLocation());
		dvo.setFileName(file.getName());
		dvo.setFileSize(FileUtil.size(file));
		Dao.insert(dvo, conn);
		log.info("Done creating raw_table_file record");
		return dvo;
	}

	private List<RawTableColDvo> createRawTableColRecords(RawTableDvo rawTableDvo, File file, Connection conn) {
		log.info("Creating raw_table_col records");
		ArrayList<RawTableColDvo> rtn = new ArrayList<RawTableColDvo>();
		List<ColumnName> colNames = ColumnNameUtil.getColumnNames(file);
		log.info("Got " + colNames.size() + " columns");
		for (ColumnName colName : colNames) {
			RawTableColDvo dvo = new RawTableColDvo();
			CosmosDvoUtil.init(dvo, "greshje", conn);
			dvo.setRawTable(rawTableDvo.getGuid());
			dvo.setColIndex(colName.getColIndex());
			dvo.setColName(colName.getColName());
			dvo.setDirtyName(colName.getDirtyName());
			rtn.add(dvo);
		}
		log.info("Doing inserts...");
		Dao.insert(rtn, conn);
		log.info("Done creating raw_table_col records");
		return rtn;
	}

	private List<RawTableGroupColDvo> addColumnsToGroup(RawTableGroupDvo groupDvo, List<RawTableColDvo> rawCols, List<RawTableGroupColDvo> existingCols, String createdByGuid, Connection conn) {
		List<RawTableGroupColDvo> rtn = RawTableGroupColProxy.addMissingCols(groupDvo.getGuid(), rawCols, existingCols, createdByGuid, conn);
		log.info("Got " + rtn.size() + " columns");
		for (RawTableGroupColDvo dvo : rtn) {
			log.info("\t" + dvo.getColName());
		}
		return rtn;
	}

	//
	// databricks methods
	//

	private void writeFileToDatabricks(File file, RawTableFileDvo fileDvo) {
		log.info("Writing file to databricks...");
		DatabricksFileUtil util = DatabricksFileUtilFactory.get();
		DatabricksFileUtilResponse resp;
		log.info("DELTEING EXISTING FILE");
		resp = util.delete(fileDvo.getFileLocation() + "/" + fileDvo.getFileName());
		log.info("Delete response: " + resp.isSuccess() + "\n" + resp.getResponse());
		log.info("DONE WITH DELETE");
		log.info("Writing file...");
		resp = util.put(fileDvo.getFileLocation(), file);
		log.info("Success: " + resp.isSuccess());
		log.info("Got response (" + resp.isSuccess() + ") " + resp.getElapsedSeconds() + " sec: " + resp.getFileName() + "\n" + resp.getResponse());
		if(resp.isSuccess() == false) {
			throw new DatabricksFileException(resp, "Did not get success response from server writing file: " + fileDvo.getFileLocation() + "/" + fileDvo.getFileName());
		}
	}

	private void createDatabricksRawDataTable(RawTableDvo dvo, RawTableFileDvo fileDvo, List<RawTableColDvo> cols, Connection conn) {
		DatabricksDbUtil.dropTable(dvo.getRawTableSchema(), dvo.getRawTableName(), conn);
		String sqlString = RawTableProxy.getDatabricksCreateTableSqlString(dvo, fileDvo, cols);
		log.info("Got sqlString: \n\n" + sqlString + "\n\n");
		log.info("Doing update...");
		Database.update(sqlString, conn);
		log.info("Table created");
	}

	//
	// getters for resources
	//

	private File getOchin() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\thumb\\OchinDemographics-thumbnail-10.csv";
		File file = new File(fileName);
		return file;
	}

	private File getAliance() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\thumb\\acdemo-thumbnail-10.csv";
		File file = new File(fileName);
		return file;
	}

	private File getDenver() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\thumb\\denverdemo-thumbnail-10.csv";
		File file = new File(fileName);
		return file;
	}

	private RawTableGroupDvo getGroup(Connection conn) {
		return Dao.find(new RawTableGroupDvo(), "code", "WOMENS_HEALTH_DEM", conn);
	}

	private String getFileLocation() {
		return "/FileStore/tables/prod/project/womens-health/raw/dem";
	}

	private List<RawTableGroupColDvo> getExistingColumns(RawTableGroupDvo groupDvo, Connection conn) {
		return Dao.findList(new RawTableGroupColDvo(), "raw_table_group", groupDvo.getGuid(), conn);
	}

}
