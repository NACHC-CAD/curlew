//
// Data Value Object (DVO) for block_def
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;

import org.yaorma.dvo.Dvo;

public class BlockDefDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "block_def";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "guid",
        "name",
        "title",
        "description",
        "project",
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
        "name",
        "title",
        "description",
        "project",
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
        "Name",
        "Title",
        "Description",
        "Project",
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
    
    private String name;
    
    private String title;
    
    private String description;
    
    private String project;
    
    private String createdBy;
    
    private String createdDate;
    
    private String updatedBy;
    
    private String updatedDate;
    
    private ProjectDvo projectDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<BlockDvo> blockBlockDefList = new ArrayList<BlockDvo>();
    
    private ArrayList<DocumentDefDvo> documentDefBlockDefList = new ArrayList<DocumentDefDvo>();
    
    //
    // trivial getters and setters
    //
    
    // guid
    
    public void setGuid(String str) {
        this.guid = str;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // name
    
    public void setName(String str) {
        this.name = str;
    }
    
    public String getName() {
        return this.name;
    }
    
    // title
    
    public void setTitle(String str) {
        this.title = str;
    }
    
    public String getTitle() {
        return this.title;
    }
    
    // description
    
    public void setDescription(String str) {
        this.description = str;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    // project
    
    public void setProject(String str) {
        this.project = str;
    }
    
    public String getProject() {
        return this.project;
    }
    
    // createdBy
    
    public void setCreatedBy(String str) {
        this.createdBy = str;
    }
    
    public String getCreatedBy() {
        return this.createdBy;
    }
    
    // createdDate
    
    public void setCreatedDate(String str) {
        this.createdDate = str;
    }
    
    public String getCreatedDate() {
        return this.createdDate;
    }
    
    // updatedBy
    
    public void setUpdatedBy(String str) {
        this.updatedBy = str;
    }
    
    public String getUpdatedBy() {
        return this.updatedBy;
    }
    
    // updatedDate
    
    public void setUpdatedDate(String str) {
        this.updatedDate = str;
    }
    
    public String getUpdatedDate() {
        return this.updatedDate;
    }
    
    // projectDvo
    
    public void setProjectDvo(ProjectDvo dvo) {
        this.projectDvo = dvo;
    }
    
    public ProjectDvo getProjectDvo() {
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
    
    public ArrayList<BlockDvo> getBlockBlockDefList() {
        return blockBlockDefList;
    }
    
    public void setBlockBlockDefList(ArrayList<BlockDvo> list) {
        this.blockBlockDefList = list;
    }
    
    public ArrayList<DocumentDefDvo> getDocumentDefBlockDefList() {
        return documentDefBlockDefList;
    }
    
    public void setDocumentDefBlockDefList(ArrayList<DocumentDefDvo> list) {
        this.documentDefBlockDefList = list;
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
