//
// Data Value Object (DVO) for document
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class DocumentDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "document";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "block",
        "created_by",
        "created_date",
        "databricks_file_name",
        "document_def",
        "file_description",
        "file_name",
        "guid",
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
        "block",
        "createdBy",
        "createdDate",
        "databricksFileName",
        "documentDef",
        "fileDescription",
        "fileName",
        "guid",
        "updatedBy",
        "updatedDate"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "Block",
        "CreatedBy",
        "CreatedDate",
        "DatabricksFileName",
        "DocumentDef",
        "FileDescription",
        "FileName",
        "Guid",
        "UpdatedBy",
        "UpdatedDate"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String block;
    
    private String createdBy;
    
    private Date createdDate;
    
    private String databricksFileName;
    
    private String documentDef;
    
    private String fileDescription;
    
    private String fileName;
    
    private String guid;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private BlockDvo blockDvo;
    
    private DocumentDefDvo documentDefDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    //
    // trivial getters and setters
    //
    
    // block
    
    public void setBlock(String val) {
        this.block = val;
    }
    
    public String getBlock() {
        return this.block;
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
    
    // databricksFileName
    
    public void setDatabricksFileName(String val) {
        this.databricksFileName = val;
    }
    
    public String getDatabricksFileName() {
        return this.databricksFileName;
    }
    
    // documentDef
    
    public void setDocumentDef(String val) {
        this.documentDef = val;
    }
    
    public String getDocumentDef() {
        return this.documentDef;
    }
    
    // fileDescription
    
    public void setFileDescription(String val) {
        this.fileDescription = val;
    }
    
    public String getFileDescription() {
        return this.fileDescription;
    }
    
    // fileName
    
    public void setFileName(String val) {
        this.fileName = val;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
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
    
    // blockDvo
    
    public void setBlockDvo(BlockDvo dvo) {
        this.blockDvo = dvo;
    }
    
    public BlockDvo getBlockDvo() {
        return this.blockDvo;
    }
    
    // documentDefDvo
    
    public void setDocumentDefDvo(DocumentDefDvo dvo) {
        this.documentDefDvo = dvo;
    }
    
    public DocumentDefDvo getDocumentDefDvo() {
        return this.documentDefDvo;
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
