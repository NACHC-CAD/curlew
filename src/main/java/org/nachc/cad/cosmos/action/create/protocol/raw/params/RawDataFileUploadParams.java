package org.nachc.cad.cosmos.action.create.protocol.raw.params;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.column.ColumnNameUtil;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.string.StringUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class RawDataFileUploadParams {

	//
	// initial parameters
	//

	// private String localHostFileRootDir;
	
	private boolean legacy = true;
	
	private String localHostFileAbsLocation;
	
	private String localHostFileAbsRoot = null;
	
	private File localDirForUpload;
	
	private File mappingFile;
	
	private String providedBy;
	
	private Date providedDate;
	
	private Set<String> rawTableGroups = new HashSet<String>();
	
	private List<RawTableGroupDvo> rawTableGroupDvoList = new ArrayList<RawTableGroupDvo>();
	
	private String projCode;
	
	private String projName;
	
	private String projDescription;

	private String orgCode;
	
	private String orgName;
	
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
	
	private boolean createNewOrg = false;
	
	private boolean createNewProject = false;
	
	private boolean overwriteExistingFiles = false;

	//
	// generated parameters
	//

	private RawTableGroupDvo rawTableGroupDvo;

	private RawTableDvo rawTableDvo;

	private RawTableFileDvo rawTableFileDvo;

	private ArrayList<RawTableColDvo> rawTableColList;

	//
	// non-trivial setters
	//
	
	public void setProvidedDate(String dateString) {
		if(dateString != null && StringUtils.isBlank(dateString) == false) {
			try {
				this.providedDate = TimeUtil.getDateForYyyy_Mm_Dd(dateString);
			} catch(Exception exp) {
				log.error("COULD NOT PARSE STRING AS DATE: " + dateString);
				exp.printStackTrace();
			}
		}
	}
	
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
		String rtn = databricksFileRoot;
		if(rtn == null) {
			return databricksFileLocation;
		}
		if(rtn.endsWith("/") == false) {
			rtn += "/";
		}
		if(rtn.endsWith(this.projCode + "/") == false) {
			rtn += this.projCode + "/";
		}
		rtn = rtn + dataGroupAbr + "/" + this.fileName;
		return rtn;
	}

	public String getDatabricksFilePathWithoutFileName() {
		return getDatabricksFilePathWithoutFileNameAndWithoutTrailingSeparator() + "/";
	}

	public String getDatabricksFilePathWithoutFileNameAndWithoutTrailingSeparator() {
		String rtn = databricksFileRoot;
		if(rtn == null) {
			return databricksFileLocation;
		}
		if(rtn.endsWith("/") == false) {
			rtn += "/";
		}
		if(rtn.endsWith(this.projCode + "/") == false) {
			rtn += this.projCode + "/";
		}
		rtn = rtn + dataGroupAbr;
		return rtn;
	}

}
