package org.nachc.cad.cosmos.action.create.protocol.raw.params;

import java.io.File;
import java.util.ArrayList;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.column.ColumnNameUtil;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RawDataFileUploadParams {

	//
	// initial parameters
	//

	private String localHostFileRootDir;
	
	private String localHostFileAbsLocation;
	
	private String projCode;
	
	private String projName;
	
	private String projDescription;

	private String orgCode;
	
	private String dataLot;

	private String protocolNamePretty;

	private String dataGroupName;

	private String dataGroupAbr;

	private String databricksFileRoot;

	private String databricksFileLocation;

	private String fileName;

	private File file;

	private String createdBy;

	private char delimiter;
	
	private boolean groupedByOrg = false;

	//
	// generated parameters
	//

	private RawTableGroupDvo rawTableGroupDvo;

	private RawTableDvo rawTableDvo;

	private RawTableFileDvo rawTableFileDvo;

	private ArrayList<RawTableColDvo> rawTableColList;

	//
	// non-trivial getters
	//

	//
	// raw_data_group stuff
	//

	public String getGroupTableSchemaName() {
		return ("prj_grp_" + this.getProjCode()).toLowerCase() + "_" + this.getDataGroupAbr();
	}

	public String getRawTableGroupDescription() {
		return "This table contains the raw data for the " + this.getDataGroupName();
	}

	public String getRawTableGroupName() {
		return this.getProtocolNamePretty() + " " + this.getDataGroupName() + " Table";
	}

	public String getRawTableGroupCode() {
		return (this.getProjCode() + "_" + this.getDataGroupAbr()).toLowerCase();
	}

	//
	// raw_table stuff
	//

	public String getRawTableSchemaName() {
		return ("prj_raw_" + this.getProjCode()).toLowerCase() + "_" + this.getDataGroupAbr();
	}

	public String getRawTableName() {
		String rtn = this.projCode + "_" + this.orgCode + "_" + this.dataGroupAbr + "_" + ColumnNameUtil.getCleanName(this.fileName);
		return rtn;
	}

	//
	// databricks stuff
	//

	public String getDatabricksFilePath() {
		return databricksFileLocation + "/" + this.fileName;
	}

}
