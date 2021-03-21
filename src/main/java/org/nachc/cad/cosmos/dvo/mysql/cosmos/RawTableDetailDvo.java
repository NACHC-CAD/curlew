//
// Data Value Object (DVO) for raw_table_detail
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableDetailDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table_detail";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "raw_table_group_guid",
        "project",
        "group_code",
        "group_name",
        "group_description",
        "group_file_location",
        "group_table_name",
        "group_table_schema",
        "raw_table_guid",
        "raw_table_name",
        "raw_table_schema",
        "file_location",
        "file_name",
        "file_size",
        "file_size_units",
        "org_code",
        "data_lot"
    };
    
    //
    // primaryKeyColumnNames
    //
    
    public static final String[] PRIMARY_KEY_COLUMN_NAMES = {
    };
    
    //
    // javaNames
    //
    
    public static final String[] JAVA_NAMES = {
        "rawTableGroupGuid",
        "project",
        "groupCode",
        "groupName",
        "groupDescription",
        "groupFileLocation",
        "groupTableName",
        "groupTableSchema",
        "rawTableGuid",
        "rawTableName",
        "rawTableSchema",
        "fileLocation",
        "fileName",
        "fileSize",
        "fileSizeUnits",
        "orgCode",
        "dataLot"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "RawTableGroupGuid",
        "Project",
        "GroupCode",
        "GroupName",
        "GroupDescription",
        "GroupFileLocation",
        "GroupTableName",
        "GroupTableSchema",
        "RawTableGuid",
        "RawTableName",
        "RawTableSchema",
        "FileLocation",
        "FileName",
        "FileSize",
        "FileSizeUnits",
        "OrgCode",
        "DataLot"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String rawTableGroupGuid;
    
    private String project;
    
    private String groupCode;
    
    private String groupName;
    
    private String groupDescription;
    
    private String groupFileLocation;
    
    private String groupTableName;
    
    private String groupTableSchema;
    
    private String rawTableGuid;
    
    private String rawTableName;
    
    private String rawTableSchema;
    
    private String fileLocation;
    
    private String fileName;
    
    private Long fileSize;
    
    private String fileSizeUnits;
    
    private String orgCode;
    
    private String dataLot;
    
    //
    // trivial getters and setters
    //
    
    // rawTableGroupGuid
    
    public void setRawTableGroupGuid(String val) {
        this.rawTableGroupGuid = val;
    }
    
    public String getRawTableGroupGuid() {
        return this.rawTableGroupGuid;
    }
    
    // project
    
    public void setProject(String val) {
        this.project = val;
    }
    
    public String getProject() {
        return this.project;
    }
    
    // groupCode
    
    public void setGroupCode(String val) {
        this.groupCode = val;
    }
    
    public String getGroupCode() {
        return this.groupCode;
    }
    
    // groupName
    
    public void setGroupName(String val) {
        this.groupName = val;
    }
    
    public String getGroupName() {
        return this.groupName;
    }
    
    // groupDescription
    
    public void setGroupDescription(String val) {
        this.groupDescription = val;
    }
    
    public String getGroupDescription() {
        return this.groupDescription;
    }
    
    // groupFileLocation
    
    public void setGroupFileLocation(String val) {
        this.groupFileLocation = val;
    }
    
    public String getGroupFileLocation() {
        return this.groupFileLocation;
    }
    
    // groupTableName
    
    public void setGroupTableName(String val) {
        this.groupTableName = val;
    }
    
    public String getGroupTableName() {
        return this.groupTableName;
    }
    
    // groupTableSchema
    
    public void setGroupTableSchema(String val) {
        this.groupTableSchema = val;
    }
    
    public String getGroupTableSchema() {
        return this.groupTableSchema;
    }
    
    // rawTableGuid
    
    public void setRawTableGuid(String val) {
        this.rawTableGuid = val;
    }
    
    public String getRawTableGuid() {
        return this.rawTableGuid;
    }
    
    // rawTableName
    
    public void setRawTableName(String val) {
        this.rawTableName = val;
    }
    
    public String getRawTableName() {
        return this.rawTableName;
    }
    
    // rawTableSchema
    
    public void setRawTableSchema(String val) {
        this.rawTableSchema = val;
    }
    
    public String getRawTableSchema() {
        return this.rawTableSchema;
    }
    
    // fileLocation
    
    public void setFileLocation(String val) {
        this.fileLocation = val;
    }
    
    public String getFileLocation() {
        return this.fileLocation;
    }
    
    // fileName
    
    public void setFileName(String val) {
        this.fileName = val;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    // fileSize
    
    public void setFileSize(Long val) {
        this.fileSize = val;
    }
    
    public Long getFileSize() {
        return this.fileSize;
    }
    
    // fileSizeUnits
    
    public void setFileSizeUnits(String val) {
        this.fileSizeUnits = val;
    }
    
    public String getFileSizeUnits() {
        return this.fileSizeUnits;
    }
    
    // orgCode
    
    public void setOrgCode(String val) {
        this.orgCode = val;
    }
    
    public String getOrgCode() {
        return this.orgCode;
    }
    
    // dataLot
    
    public void setDataLot(String val) {
        this.dataLot = val;
    }
    
    public String getDataLot() {
        return this.dataLot;
    }
    
    //
    // implementation of Dvo
    //
    
    public String getTableName() {
        return TABLE_NAME;
    };
    
    public String getSchemaName() {
        return SCHEMA_NAME;
    };
    
    public String[] getColumnNames() {
        return COLUMN_NAMES;
    };
    
    public String[] getPrimaryKeyColumnNames() {
        return PRIMARY_KEY_COLUMN_NAMES;
    };
    
    public String[] getJavaNames() {
        return JAVA_NAMES;
    };
    
    public String[] getJavaNamesProper() {
        return JAVA_NAMES_PROPER;
    };
    
    public void setDescriptions(HashMap<String, String> descriptions) {
        this.descriptions = descriptions;
    }
    
    public HashMap<String, String> getDescriptions() {
        return this.descriptions;
    }
    
    public void addDescription(String javaName, String value) {
        this.descriptions.put(javaName, value);
    }
    
    public String getDescription(String javaName) {
        return this.descriptions.get(javaName);
    }
    
    public String[] getPrimaryKeyValues() {
        String[] rtn = new String[] {
        };
        return rtn;
    }
}
