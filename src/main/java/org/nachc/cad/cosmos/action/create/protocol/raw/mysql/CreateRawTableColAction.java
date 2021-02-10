package org.nachc.cad.cosmos.action.create.protocol.raw.mysql;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.util.column.ColumnName;
import org.nachc.cad.cosmos.util.column.ColumnNameUtil;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableColAction {

	public static void execute(RawDataFileUploadParams params, Connection conn) {
		log.info("Creating raw_table_col records");
		ArrayList<RawTableColDvo> rtn = new ArrayList<RawTableColDvo>();
		List<ColumnName> colNames = ColumnNameUtil.getColumnNames(params.getFile(), params.getDelimiter());
		log.info("Got " + colNames.size() + " columns");
		for (ColumnName colName : colNames) {
			RawTableColDvo dvo = new RawTableColDvo();
			CosmosDvoUtil.init(dvo, "greshje", conn);
			dvo.setRawTable(params.getRawTableDvo().getGuid());
			dvo.setColIndex(colName.getColIndex());
			dvo.setColName(colName.getColName());
			dvo.setDirtyName(colName.getDirtyName());
			dvo.setProject(params.getProjCode());
			doInsert(dvo, conn);
			rtn.add(dvo);
		}
		log.info("Done creating raw_table_col records: " + rtn.size() + " records created.");
		params.setRawTableColList(rtn);
	}

	private static void doInsert(RawTableColDvo dvo, Connection conn) {
		String rootColName = dvo.getColName();
		for(int i=0;i<10;i++) {
			// allow for upto 10 columns with the same name
			try {
				log.info("Doing insert for: " + dvo.getTableName() + "." + dvo.getColName());
				Dao.insert(dvo, conn);
				log.info("Successful insert for: " + dvo.getTableName() + "." + dvo.getColName());
				break;
			} catch(Exception exp) {
				if(i<9) {
					dvo.setColName(rootColName + "_" + i);
					continue;
				} else {
					throw(new RuntimeException (exp));
				}
			}
		}
	}

}
