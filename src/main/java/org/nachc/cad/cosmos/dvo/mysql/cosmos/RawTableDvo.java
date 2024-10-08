//
// Data Value Object (DVO) for raw_table
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "created_by",
        "created_date",
        "guid",
        "project",
        "raw_table_group",
        "raw_table_name",
        "raw_table_schema",
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
        "createdBy",
        "createdDate",
        "guid",
        "project",
        "rawTableGroup",
        "rawTableName",
        "rawTableSchema",
        "updatedBy",
        "updatedDate"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "CreatedBy",
        "CreatedDate",
        "Guid",
        "Project",
        "RawTableGroup",
        "RawTableName",
        "RawTableSchema",
        "UpdatedBy",
        "UpdatedDate"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String createdBy;
    
    private Date createdDate;
    
    private String guid;
    
    private String project;
    
    private String rawTableGroup;
    
    private String rawTableName;
    
    private String rawTableSchema;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private RawTableGroupDvo rawTableGroupDvo;
    
    private ProjCodeDvo projectDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<RawTableColDvo> rawTableColRawTableList = new ArrayList<RawTableColDvo>();
    
    private ArrayList<RawTableFileDvo> rawTableFileRawTableList = new ArrayList<RawTableFileDvo>();
    
    //
    // trivial getters and setters
    //
    
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
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // project
    
    public void setProject(String val) {
        this.project = val;
    }
    
    public String getProject() {
        return this.project;
    }
    
    // rawTableGroup
    
    public void setRawTableGroup(String val) {
        this.rawTableGroup = val;
    }
    
    public String getRawTableGroup() {
        return this.rawTableGroup;
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
    
    // rawTableGroupDvo
    
    public void setRawTableGroupDvo(RawTableGroupDvo dvo) {
        this.rawTableGroupDvo = dvo;
    }
    
    public RawTableGroupDvo getRawTableGroupDvo() {
        return this.rawTableGroupDvo;
    }
    
    // projectDvo
    
    public void setProjectDvo(ProjCodeDvo dvo) {
        this.projectDvo = dvo;
    }
    
    public ProjCodeDvo getProjectDvo() {
        return this.projectDvo;
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
    
    public ArrayList<RawTableColDvo> getRawTableColRawTableList() {
        return rawTableColRawTableList;
    }
    
    public void setRawTableColRawTableList(ArrayList<RawTableColDvo> list) {
        this.rawTableColRawTableList = list;
    }
    
    public ArrayList<RawTableFileDvo> getRawTableFileRawTableList() {
        return rawTableFileRawTableList;
    }
    
    public void setRawTableFileRawTableList(ArrayList<RawTableFileDvo> list) {
        this.rawTableFileRawTableList = list;
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
