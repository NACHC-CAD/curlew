//
// Data Value Object (DVO) for block
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

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
        "block_def",
        "created_by",
        "created_date",
        "description",
        "guid",
        "status",
        "title",
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
        "blockDef",
        "createdBy",
        "createdDate",
        "description",
        "guid",
        "status",
        "title",
        "updatedBy",
        "updatedDate"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "BlockDef",
        "CreatedBy",
        "CreatedDate",
        "Description",
        "Guid",
        "Status",
        "Title",
        "UpdatedBy",
        "UpdatedDate"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String blockDef;
    
    private String createdBy;
    
    private Date createdDate;
    
    private String description;
    
    private String guid;
    
    private String status;
    
    private String title;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private BlockDefDvo blockDefDvo;
    
    private StatusDvo statusDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<DocumentDvo> documentBlockList = new ArrayList<DocumentDvo>();
    
    //
    // trivial getters and setters
    //
    
    // blockDef
    
    public void setBlockDef(String val) {
        this.blockDef = val;
    }
    
    public String getBlockDef() {
        return this.blockDef;
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
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // status
    
    public void setStatus(String val) {
        this.status = val;
    }
    
    public String getStatus() {
        return this.status;
    }
    
    // title
    
    public void setTitle(String val) {
        this.title = val;
    }
    
    public String getTitle() {
        return this.title;
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
