//
// Data Value Object (DVO) for raw_table_col_alias
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableColAliasDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table_col_alias";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "group_code",
        "raw_table_schema",
        "raw_table_name",
        "col_name",
        "col_alias",
        "raw_table",
        "raw_table_group",
        "raw_table_file",
        "raw_table_col"
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
        "groupCode",
        "rawTableSchema",
        "rawTableName",
        "colName",
        "colAlias",
        "rawTable",
        "rawTableGroup",
        "rawTableFile",
        "rawTableCol"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "GroupCode",
        "RawTableSchema",
        "RawTableName",
        "ColName",
        "ColAlias",
        "RawTable",
        "RawTableGroup",
        "RawTableFile",
        "RawTableCol"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String groupCode;
    
    private String rawTableSchema;
    
    private String rawTableName;
    
    private String colName;
    
    private String colAlias;
    
    private String rawTable;
    
    private String rawTableGroup;
    
    private String rawTableFile;
    
    private String rawTableCol;
    
    //
    // trivial getters and setters
    //
    
    // groupCode
    
    public void setGroupCode(String val) {
        this.groupCode = val;
    }
    
    public String getGroupCode() {
        return this.groupCode;
    }
    
    // rawTableSchema
    
    public void setRawTableSchema(String val) {
        this.rawTableSchema = val;
    }
    
    public String getRawTableSchema() {
        return this.rawTableSchema;
    }
    
    // rawTableName
    
    public void setRawTableName(String val) {
        this.rawTableName = val;
    }
    
    public String getRawTableName() {
        return this.rawTableName;
    }
    
    // colName
    
    public void setColName(String val) {
        this.colName = val;
    }
    
    public String getColName() {
        return this.colName;
    }
    
    // colAlias
    
    public void setColAlias(String val) {
        this.colAlias = val;
    }
    
    public String getColAlias() {
        return this.colAlias;
    }
    
    // rawTable
    
    public void setRawTable(String val) {
        this.rawTable = val;
    }
    
    public String getRawTable() {
        return this.rawTable;
    }
    
    // rawTableGroup
    
    public void setRawTableGroup(String val) {
        this.rawTableGroup = val;
    }
    
    public String getRawTableGroup() {
        return this.rawTableGroup;
    }
    
    // rawTableFile
    
    public void setRawTableFile(String val) {
        this.rawTableFile = val;
    }
    
    public String getRawTableFile() {
        return this.rawTableFile;
    }
    
    // rawTableCol
    
    public void setRawTableCol(String val) {
        this.rawTableCol = val;
    }
    
    public String getRawTableCol() {
        return this.rawTableCol;
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
