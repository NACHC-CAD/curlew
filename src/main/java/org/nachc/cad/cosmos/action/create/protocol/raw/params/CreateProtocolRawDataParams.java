package org.nachc.cad.cosmos.action.create.protocol.raw.params;

import java.io.File;
import java.util.ArrayList;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupRawTableDvo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateProtocolRawDataParams {

	//
	// initial parameters
	//

	private String protocolName;

	private String protocolNamePretty;

	private String dataGroupName;

	private String dataGroupAbr;

	private String databricksFileLocation;

	private String databricksFileName;

	private File file;

	private String createdBy;

	private char delimiter;

	//
	// generated parameters
	//

	private RawTableGroupDvo rawTableGroupDvo;

	private RawTableDvo rawTableDvo;

	private RawTableFileDvo rawTableFileDvo;

	private ArrayList<RawTableColDvo> rawTableColList;

	private RawTableGroupRawTableDvo rawTableGroupRawTableDvo;

	//
	// non-trivial getters
	//

	//
	// databricks stuff
	//

	public String getDatabricksFilePath() {
		return databricksFileLocation + "/" + databricksFileName;
	}

	//
	// raw_data_group stuff
	//

	public String getGroupTableSchemaName() {
		return ("prj_grp_" + this.getProtocolName()).toLowerCase();
	}

	public String getRawTableGroupDescription() {
		return "This table contains the raw data for the " + this.getDataGroupName();
	}

	public String getRawTableGroupName() {
		return this.getProtocolNamePretty() + " " + this.getDataGroupName() + " Table";
	}

	public String getRawTableGroupCode() {
		return (this.getProtocolName() + "_" + this.getDataGroupAbr()).toUpperCase();
	}

	//
	// raw_table stuff
	//

	public String getRawTableSchemaName() {
		return ("prj_raw_" + this.getProtocolName()).toLowerCase();
	}

	public String getRawTableName() {
		return (protocolName + "_" + dataGroupAbr).toLowerCase();
	}

}
