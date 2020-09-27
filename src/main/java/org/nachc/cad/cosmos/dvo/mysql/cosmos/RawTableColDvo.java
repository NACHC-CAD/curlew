//
// Data Value Object (DVO) for raw_table_col
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableColDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table_col";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "guid",
        "raw_table",
        "col_index",
        "dirty_name",
        "col_name",
        "col_alias",
        "created_by",
        "created_date",
        "updated_by",
        "updated_date"
    };
    
    //
    // primaryKeyColumnNames
    //
    
    public static final String[] PRIMARY_KEY_COLUMN_NAMES = {
        "guid"
    };
    
    //
    // javaNames
    //
    
    public static final String[] JAVA_NAMES = {
        "guid",
        "rawTable",
        "colIndex",
        "dirtyName",
        "colName",
        "colAlias",
        "createdBy",
        "createdDate",
        "updatedBy",
        "updatedDate"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "Guid",
        "RawTable",
        "ColIndex",
        "DirtyName",
        "ColName",
        "ColAlias",
        "CreatedBy",
        "CreatedDate",
        "UpdatedBy",
        "UpdatedDate"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String guid;
    
    private String rawTable;
    
    private Integer colIndex;
    
    private String dirtyName;
    
    private String colName;
    
    private String colAlias;
    
    private String createdBy;
    
    private Date createdDate;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private RawTableDvo rawTableDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    //
    // trivial getters and setters
    //
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // rawTable
    
    public void setRawTable(String val) {
        this.rawTable = val;
    }
    
    public String getRawTable() {
        return this.rawTable;
    }
    
    // colIndex
    
    public void setColIndex(Integer val) {
        this.colIndex = val;
    }
    
    public Integer getColIndex() {
        return this.colIndex;
    }
    
    // dirtyName
    
    public void setDirtyName(String val) {
        this.dirtyName = val;
    }
    
    public String getDirtyName() {
        return this.dirtyName;
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
    
    // createdBy
    
    public void setCreatedBy(String val) {
        this.createdBy = val;
    }
    
    public String getCreatedBy() {
        return this.createdBy;
    }
    
    // createdDate
    
    public void setCreatedDate(Date val) {
        this.createdDate = val;
    }
    
    public Date getCreatedDate() {
        return this.createdDate;
    }
    
    // updatedBy
    
    public void setUpdatedBy(String val) {
        this.updatedBy = val;
    }
    
    public String getUpdatedBy() {
        return this.updatedBy;
    }
    
    // updatedDate
    
    public void setUpdatedDate(Date val) {
        this.updatedDate = val;
    }
    
    public Date getUpdatedDate() {
        return this.updatedDate;
    }
    
    // rawTableDvo
    
    public void setRawTableDvo(RawTableDvo dvo) {
        this.rawTableDvo = dvo;
    }
    
    public RawTableDvo getRawTableDvo() {
        return this.rawTableDvo;
    }
    
    // createdByDvo
    
    public void setCreatedByDvo(PersonDvo dvo) {
        this.createdByDvo = dvo;
    }
    
    public PersonDvo getCreatedByDvo() {
        return this.createdByDvo;
    }
    
    // updatedByDvo
    
    public void setUpdatedByDvo(PersonDvo dvo) {
        this.updatedByDvo = dvo;
    }
    
    public PersonDvo getUpdatedByDvo() {
        return this.updatedByDvo;
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
            getGuid()
        };
        return rtn;
    }
}
