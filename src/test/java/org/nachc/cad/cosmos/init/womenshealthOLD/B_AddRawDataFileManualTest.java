package org.nachc.cad.cosmos.init.womenshealthOLD;

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
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupRawTableDvo;
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
public class B_AddRawDataFileManualTest {

	@Test
	public void shouldAddRawDataFile() {
		log.info("Starting test...");
		// get the connection and the created by record
		log.info("Getting connection...");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		// get databricks conn
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("DOING rmdir (COMMENT THIS OUT)");
		DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().rmdir(this.getFileLocation());
		log.info("Got response (" + resp.isSuccess() + "): " + resp.getStatusCode() + " \n" + resp.getResponse());
		log.info("DROPPING WOMENS HEALTH SCHEMAS");
		log.info("Creating...");
		DatabricksDbUtil.dropDatabase("prj_raw_womens_health", dbConn);
		log.info("Dropping...");
		DatabricksDbUtil.dropDatabase("prj_grp_womens_health", dbConn);
		log.info("Dropping...");
		DatabricksDbUtil.createDatabase("prj_raw_womens_health", dbConn);
		log.info("Creating...");
		DatabricksDbUtil.createDatabase("prj_grp_womens_health", dbConn);
		log.info("Done with rmdir/drop schemas");
		// get the file
		File file = getOchin();
		// get the createdBy record
		PersonDvo createdBy = PersonProxy.getForUid("greshje", mySqlConn);
		// create the records for the incoming file
		addFile(getOchin(), createdBy, mySqlConn, dbConn);
		addFile(getAliance(), createdBy, mySqlConn, dbConn);
		addFile(getDenver(), createdBy, mySqlConn, dbConn);
		setAliases(mySqlConn);
		Database.commit(mySqlConn);
		// done
		log.info("Done.");
	}

	private void addFile(File file, PersonDvo createdBy, Connection mysqlConn, Connection dbConn) {
		log.info("Creating records...");
		// mysql stuff
		RawTableDvo tableDvo = createRawTableRecord(file, mysqlConn);
		RawTableFileDvo fileDvo = createRawTableFileRecord(tableDvo, file, mysqlConn);
		List<RawTableColDvo> rawCols = createRawTableColRecords(tableDvo, file, mysqlConn);
		RawTableGroupDvo groupDvo = getGroup(mysqlConn);
		List<RawTableGroupColDvo> existingCols = getExistingColumns(groupDvo, mysqlConn);
		List<RawTableGroupColDvo> allCols = addColumnsToGroup(groupDvo, rawCols, existingCols, createdBy.getGuid(), mysqlConn);
		addToGroup(groupDvo, tableDvo, createdBy.getGuid(), mysqlConn);
		// databricks stuff
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

	private void addToGroup(RawTableGroupDvo groupDvo, RawTableDvo tableDvo, String createdByGuid, Connection mysqlConn) {
		// (JEG) START HERE
		log.info("Createing group record");
		RawTableGroupRawTableDvo dvo = new RawTableGroupRawTableDvo();
		CosmosDvoUtil.init(dvo, createdByGuid);
		dvo.setRawTable(tableDvo.getGuid());
		dvo.setRawTableGroup(groupDvo.getGuid());
		Dao.insert(dvo, mysqlConn);
		log.info("Group record created.");
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
		if (resp.isSuccess() == false) {
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
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\OchinDemographics.csv";
		File file = new File(fileName);
		return file;
	}

	private File getAliance() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\acdemo.csv";
		File file = new File(fileName);
		return file;
	}

	private File getDenver() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\denverdemo.csv";
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

	//
	// one offs (update to demonstrate column aliases
	//

	private void setAliases(Connection conn) {
		log.info("Setting aliases");
		String sqlString = "update cosmos.raw_table_col set col_alias = ? where col_name = ?";
		Database.update(sqlString, new String[] { "access_to_care", "accessto_care" }, conn);
		Database.update(sqlString, new String[] { "age", "age_at_the_endof_measurement_year" }, conn);
		Database.update(sqlString, new String[] { "education", "education_level" }, conn);
		Database.update(sqlString, new String[] { "ethnicity", "ethnicity_standard_description" }, conn);
		Database.update(sqlString, new String[] { "sex", "gender" }, conn);
		Database.update(sqlString, new String[] { "insurance", "health_insurance_type" }, conn);
		Database.update(sqlString, new String[] { "m1n", "m1num" }, conn);
		Database.update(sqlString, new String[] { "race", "race_standard_descr" }, conn);
		Database.update(sqlString, new String[] { "transportation", "transporation" }, conn);
		log.info("Done setting aliases");
	}

}
