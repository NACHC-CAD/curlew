package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.util.column.ColumnName;
import org.nachc.cad.cosmos.util.column.ColumnNameUtil;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableColAction {

	public static void execute(CreateProtocolRawDataParams params, Connection conn) {
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
			rtn.add(dvo);
		}
		log.info("Doing inserts...");
		Dao.insert(rtn, conn);
		log.info("Done creating raw_table_col records: " + rtn.size() + " records created.");
		params.setRawTableColList(rtn);
	}

}
