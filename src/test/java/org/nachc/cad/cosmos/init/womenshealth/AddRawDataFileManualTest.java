package org.nachc.cad.cosmos.init.womenshealth;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.column.ColumnName;
import org.nachc.cad.cosmos.util.column.ColumnNameUtil;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddRawDataFileManualTest {

	@Test
	public void shouldAddRawDataFile() {
		log.info("Starting test...");
		// get the file
		File file = getFile();
		// get the connection
		log.info("Getting connection...");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		// create the records for the incoming file
		log.info("Creating records...");
		RawTableDvo tableDvo = createRawTableRecord(file, conn);
		RawTableFileDvo fileDvo = createRawTableFileRecord(tableDvo, file, conn);
		List<RawTableColDvo> colDvoList = createRawTableColRecords(tableDvo, file, conn);
		RawTableGroupDvo groupDvo = getGroup(conn);
		// done
		log.info("Done.");
	}

	private RawTableGroupDvo getGroup(Connection conn) {
		return Dao.find(new RawTableGroupDvo(), "code", "WOMENS_HEALTH_DEM", conn);
	}

	private String getFileLocation() {
		return "/FileStore/tables/prod/project/womens-health/raw/dem";
	}

	private File getFile() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\thumb\\OchinDemographics-thumbnail-10.csv";
		File file = new File(fileName);
		return file;
	}

	private RawTableDvo createRawTableRecord(File file, Connection conn) {
		log.info("Creating raw_table record");
		RawTableDvo dvo = new RawTableDvo();
		CosmosDvoUtil.init(dvo, "greshje", conn);
		dvo.setRawTableSchema("prj_raw_womens_health");
		dvo.setRawTableName(file.getName());
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

	private List<RawTableGroupColDvo> getExistingColumns(RawTableGroupDvo groupDvo, Connection conn) {
		return Dao.findList(new RawTableGroupColDvo(), "raw_table", groupDvo.getGuid(), conn);
	}

}
