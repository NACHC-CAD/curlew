//
// Data Value Object (DVO) for raw_table_col_detail
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableColDetailDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table_col_detail";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "col_alias",
        "col_index",
        "col_name",
        "data_lot",
        "dirty_name",
        "file_location",
        "file_name",
        "file_size",
        "file_size_units",
        "group_file_location",
        "group_raw_table_schema",
        "group_table_name",
        "group_table_schema",
        "org_code",
        "project",
        "raw_table",
        "raw_table_col",
        "raw_table_file",
        "raw_table_group",
        "raw_table_group_code",
        "raw_table_group_desc",
        "raw_table_group_name",
        "raw_table_name",
        "raw_table_schema",
        "real_name"
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
        "colAlias",
        "colIndex",
        "colName",
        "dataLot",
        "dirtyName",
        "fileLocation",
        "fileName",
        "fileSize",
        "fileSizeUnits",
        "groupFileLocation",
        "groupRawTableSchema",
        "groupTableName",
        "groupTableSchema",
        "orgCode",
        "project",
        "rawTable",
        "rawTableCol",
        "rawTableFile",
        "rawTableGroup",
        "rawTableGroupCode",
        "rawTableGroupDesc",
        "rawTableGroupName",
        "rawTableName",
        "rawTableSchema",
        "realName"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "ColAlias",
        "ColIndex",
        "ColName",
        "DataLot",
        "DirtyName",
        "FileLocation",
        "FileName",
        "FileSize",
        "FileSizeUnits",
        "GroupFileLocation",
        "GroupRawTableSchema",
        "GroupTableName",
        "GroupTableSchema",
        "OrgCode",
        "Project",
        "RawTable",
        "RawTableCol",
        "RawTableFile",
        "RawTableGroup",
        "RawTableGroupCode",
        "RawTableGroupDesc",
        "RawTableGroupName",
        "RawTableName",
        "RawTableSchema",
        "RealName"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String colAlias;
    
    private Integer colIndex;
    
    private String colName;
    
    private String dataLot;
    
    private String dirtyName;
    
    private String fileLocation;
    
    private String fileName;
    
    private Long fileSize;
    
    private String fileSizeUnits;
    
    private String groupFileLocation;
    
    private String groupRawTableSchema;
    
    private String groupTableName;
    
    private String groupTableSchema;
    
    private String orgCode;
    
    private String project;
    
    private String rawTable;
    
    private String rawTableCol;
    
    private String rawTableFile;
    
    private String rawTableGroup;
    
    private String rawTableGroupCode;
    
    private String rawTableGroupDesc;
    
    private String rawTableGroupName;
    
    private String rawTableName;
    
    private String rawTableSchema;
    
    private String realName;
    
    //
    // trivial getters and setters
    //
    
    // colAlias
    
    public void setColAlias(String val) {
        this.colAlias = val;
    }
    
    public String getColAlias() {
        return this.colAlias;
    }
    
    // colIndex
    
    public void setColIndex(Integer val) {
        this.colIndex = val;
    }
    
    public Integer getColIndex() {
        return this.colIndex;
    }
    
    // colName
    
    public void setColName(String val) {
        this.colName = val;
    }
    
    public String getColName() {
        return this.colName;
    }
    
    // dataLot
    
    public void setDataLot(String val) {
        this.dataLot = val;
    }
    
    public String getDataLot() {
        return this.dataLot;
    }
    
    // dirtyName
    
    public void setDirtyName(String val) {
        this.dirtyName = val;
    }
    
    public String getDirtyName() {
        return this.dirtyName;
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
    
    // groupFileLocation
    
    public void setGroupFileLocation(String val) {
        this.groupFileLocation = val;
    }
    
    public String getGroupFileLocation() {
        return this.groupFileLocation;
    }
    
    // groupRawTableSchema
    
    public void setGroupRawTableSchema(String val) {
        this.groupRawTableSchema = val;
    }
    
    public String getGroupRawTableSchema() {
        return this.groupRawTableSchema;
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
    
    // rawTable
    
    public void setRawTable(String val) {
        this.rawTable = val;
    }
    
    public String getRawTable() {
        return this.rawTable;
    }
    
    // rawTableCol
    
    public void setRawTableCol(String val) {
        this.rawTableCol = val;
    }
    
    public String getRawTableCol() {
        return this.rawTableCol;
    }
    
    // rawTableFile
    
    public void setRawTableFile(String val) {
        this.rawTableFile = val;
    }
    
    public String getRawTableFile() {
        return this.rawTableFile;
    }
    
    // rawTableGroup
    
    public void setRawTableGroup(String val) {
        this.rawTableGroup = val;
    }
    
    public String getRawTableGroup() {
        return this.rawTableGroup;
    }
    
    // rawTableGroupCode
    
    public void setRawTableGroupCode(String val) {
        this.rawTableGroupCode = val;
    }
    
    public String getRawTableGroupCode() {
        return this.rawTableGroupCode;
    }
    
    // rawTableGroupDesc
    
    public void setRawTableGroupDesc(String val) {
        this.rawTableGroupDesc = val;
    }
    
    public String getRawTableGroupDesc() {
        return this.rawTableGroupDesc;
    }
    
    // rawTableGroupName
    
    public void setRawTableGroupName(String val) {
        this.rawTableGroupName = val;
    }
    
    public String getRawTableGroupName() {
        return this.rawTableGroupName;
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
    
    // realName
    
    public void setRealName(String val) {
        this.realName = val;
    }
    
    public String getRealName() {
        return this.realName;
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
