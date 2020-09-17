//
// Data Value Object (DVO) for block
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;

import org.yaorma.dvo.Dvo;

public class BlockDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "block";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "guid",
        "title",
        "description",
        "block_def",
        "status",
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
        "title",
        "description",
        "blockDef",
        "status",
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
        "Title",
        "Description",
        "BlockDef",
        "Status",
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
    
    private String title;
    
    private String description;
    
    private String blockDef;
    
    private String status;
    
    private String createdBy;
    
    private String createdDate;
    
    private String updatedBy;
    
    private String updatedDate;
    
    private BlockDefDvo blockDefDvo;
    
    private StatusDvo statusDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<DocumentDvo> documentBlockList = new ArrayList<DocumentDvo>();
    
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
    
    // blockDef
    
    public void setBlockDef(String str) {
        this.blockDef = str;
    }
    
    public String getBlockDef() {
        return this.blockDef;
    }
    
    // status
    
    public void setStatus(String str) {
        this.status = str;
    }
    
    public String getStatus() {
        return this.status;
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
    
    // blockDefDvo
    
    public void setBlockDefDvo(BlockDefDvo dvo) {
        this.blockDefDvo = dvo;
    }
    
    public BlockDefDvo getBlockDefDvo() {
        return this.blockDefDvo;
    }
    
    // statusDvo
    
    public void setStatusDvo(StatusDvo dvo) {
        this.statusDvo = dvo;
    }
    
    public StatusDvo getStatusDvo() {
        return this.statusDvo;
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
    
    public ArrayList<DocumentDvo> getDocumentBlockList() {
        return documentBlockList;
    }
    
    public void setDocumentBlockList(ArrayList<DocumentDvo> list) {
        this.documentBlockList = list;
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
