//
// Data Value Object (DVO) for raw_table_group
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableGroupDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table_group";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "code",
        "created_by",
        "created_date",
        "description",
        "file_location",
        "group_table_name",
        "group_table_schema",
        "guid",
        "name",
        "project",
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
        "code",
        "createdBy",
        "createdDate",
        "description",
        "fileLocation",
        "groupTableName",
        "groupTableSchema",
        "guid",
        "name",
        "project",
        "rawTableSchema",
        "updatedBy",
        "updatedDate"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "Code",
        "CreatedBy",
        "CreatedDate",
        "Description",
        "FileLocation",
        "GroupTableName",
        "GroupTableSchema",
        "Guid",
        "Name",
        "Project",
        "RawTableSchema",
        "UpdatedBy",
        "UpdatedDate"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String code;
    
    private String createdBy;
    
    private Date createdDate;
    
    private String description;
    
    private String fileLocation;
    
    private String groupTableName;
    
    private String groupTableSchema;
    
    private String guid;
    
    private String name;
    
    private String project;
    
    private String rawTableSchema;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private ProjCodeDvo projectDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<RawTableDvo> rawTableRawTableGroupList = new ArrayList<RawTableDvo>();
    
    //
    // trivial getters and setters
    //
    
    // code
    
    public void setCode(String val) {
        this.code = val;
    }
    
    public String getCode() {
        return this.code;
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
    
    // description
    
    public void setDescription(String val) {
        this.description = val;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    // fileLocation
    
    public void setFileLocation(String val) {
        this.fileLocation = val;
    }
    
    public String getFileLocation() {
        return this.fileLocation;
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
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // name
    
    public void setName(String val) {
        this.name = val;
    }
    
    public String getName() {
        return this.name;
    }
    
    // project
    
    public void setProject(String val) {
        this.project = val;
    }
    
    public String getProject() {
        return this.project;
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
    
    public ArrayList<RawTableDvo> getRawTableRawTableGroupList() {
        return rawTableRawTableGroupList;
    }
    
    public void setRawTableRawTableGroupList(ArrayList<RawTableDvo> list) {
        this.rawTableRawTableGroupList = list;
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
