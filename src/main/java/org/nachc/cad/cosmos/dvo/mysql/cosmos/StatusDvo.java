//
// Data Value Object (DVO) for status
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class StatusDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "status";
    
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
        "name",
        "updated_by",
        "updated_date"
    };
    
    //
    // primaryKeyColumnNames
    //
    
    public static final String[] PRIMARY_KEY_COLUMN_NAMES = {
        "code"
    };
    
    //
    // javaNames
    //
    
    public static final String[] JAVA_NAMES = {
        "code",
        "createdBy",
        "createdDate",
        "description",
        "name",
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
        "Name",
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
    
    private String name;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<BlockDvo> blockStatusList = new ArrayList<BlockDvo>();
    
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
    
    // name
    
    public void setName(String val) {
        this.name = val;
    }
    
    public String getName() {
        return this.name;
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
    
    public ArrayList<BlockDvo> getBlockStatusList() {
        return blockStatusList;
    }
    
    public void setBlockStatusList(ArrayList<BlockDvo> list) {
        this.blockStatusList = list;
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
            getCode()
        };
        return rtn;
    }
}
