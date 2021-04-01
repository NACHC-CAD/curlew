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
        "data_lot",
        "file_location",
        "file_name",
        "file_size",
        "file_size_units",
        "group_code",
        "group_description",
        "group_file_location",
        "group_name",
        "group_table_name",
        "group_table_schema",
        "org_code",
        "project",
        "raw_table_group_guid",
        "raw_table_guid",
        "raw_table_name",
        "raw_table_schema"
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
        "dataLot",
        "fileLocation",
        "fileName",
        "fileSize",
        "fileSizeUnits",
        "groupCode",
        "groupDescription",
        "groupFileLocation",
        "groupName",
        "groupTableName",
        "groupTableSchema",
        "orgCode",
        "project",
        "rawTableGroupGuid",
        "rawTableGuid",
        "rawTableName",
        "rawTableSchema"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "DataLot",
        "FileLocation",
        "FileName",
        "FileSize",
        "FileSizeUnits",
        "GroupCode",
        "GroupDescription",
        "GroupFileLocation",
        "GroupName",
        "GroupTableName",
        "GroupTableSchema",
        "OrgCode",
        "Project",
        "RawTableGroupGuid",
        "RawTableGuid",
        "RawTableName",
        "RawTableSchema"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String dataLot;
    
    private String fileLocation;
    
    private String fileName;
    
    private Long fileSize;
    
    private String fileSizeUnits;
    
    private String groupCode;
    
    private String groupDescription;
    
    private String groupFileLocation;
    
    private String groupName;
    
    private String groupTableName;
    
    private String groupTableSchema;
    
    private String orgCode;
    
    private String project;
    
    private String rawTableGroupGuid;
    
    private String rawTableGuid;
    
    private String rawTableName;
    
    private String rawTableSchema;
    
    //
    // trivial getters and setters
    //
    
    // dataLot
    
    public void setDataLot(String val) {
        this.dataLot = val;
    }
    
    public String getDataLot() {
        return this.dataLot;
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
    
    // groupCode
    
    public void setGroupCode(String val) {
        this.groupCode = val;
    }
    
    public String getGroupCode() {
        return this.groupCode;
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
    
    // groupName
    
    public void setGroupName(String val) {
        this.groupName = val;
    }
    
    public String getGroupName() {
        return this.groupName;
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
    
    // orgCode
    
    public void setOrgCode(String val) {
        this.orgCode = val;
    }
    
    public String getOrgCode() {
        return this.orgCode;
    }
    
    // project
    
    public void setProject(String val) {
        this.project = val;
    }
    
    public String getProject() {
        return this.project;
    }
    
    // rawTableGroupGuid
    
    public void setRawTableGroupGuid(String val) {
        this.rawTableGroupGuid = val;
    }
    
    public String getRawTableGroupGuid() {
        return this.rawTableGroupGuid;
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
