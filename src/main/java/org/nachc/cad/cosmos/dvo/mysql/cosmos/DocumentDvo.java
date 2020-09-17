//
// Data Value Object (DVO) for document
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;

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
        "guid",
        "block",
        "file_name",
        "file_description",
        "file_type",
        "document_role",
        "databricks_dir",
        "document_def",
        "databricks_file_name",
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
        "block",
        "fileName",
        "fileDescription",
        "fileType",
        "documentRole",
        "databricksDir",
        "documentDef",
        "databricksFileName",
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
        "Block",
        "FileName",
        "FileDescription",
        "FileType",
        "DocumentRole",
        "DatabricksDir",
        "DocumentDef",
        "DatabricksFileName",
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
    
    private String block;
    
    private String fileName;
    
    private String fileDescription;
    
    private String fileType;
    
    private String documentRole;
    
    private String databricksDir;
    
    private String documentDef;
    
    private String databricksFileName;
    
    private String createdBy;
    
    private String createdDate;
    
    private String updatedBy;
    
    private String updatedDate;
    
    private BlockDvo blockDvo;
    
    private FileTypeDvo fileTypeDvo;
    
    private DocumentRoleDvo documentRoleDvo;
    
    private DocumentDefDvo documentDefDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
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
    
    // block
    
    public void setBlock(String str) {
        this.block = str;
    }
    
    public String getBlock() {
        return this.block;
    }
    
    // fileName
    
    public void setFileName(String str) {
        this.fileName = str;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    // fileDescription
    
    public void setFileDescription(String str) {
        this.fileDescription = str;
    }
    
    public String getFileDescription() {
        return this.fileDescription;
    }
    
    // fileType
    
    public void setFileType(String str) {
        this.fileType = str;
    }
    
    public String getFileType() {
        return this.fileType;
    }
    
    // documentRole
    
    public void setDocumentRole(String str) {
        this.documentRole = str;
    }
    
    public String getDocumentRole() {
        return this.documentRole;
    }
    
    // databricksDir
    
    public void setDatabricksDir(String str) {
        this.databricksDir = str;
    }
    
    public String getDatabricksDir() {
        return this.databricksDir;
    }
    
    // documentDef
    
    public void setDocumentDef(String str) {
        this.documentDef = str;
    }
    
    public String getDocumentDef() {
        return this.documentDef;
    }
    
    // databricksFileName
    
    public void setDatabricksFileName(String str) {
        this.databricksFileName = str;
    }
    
    public String getDatabricksFileName() {
        return this.databricksFileName;
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
    
    // blockDvo
    
    public void setBlockDvo(BlockDvo dvo) {
        this.blockDvo = dvo;
    }
    
    public BlockDvo getBlockDvo() {
        return this.blockDvo;
    }
    
    // fileTypeDvo
    
    public void setFileTypeDvo(FileTypeDvo dvo) {
        this.fileTypeDvo = dvo;
    }
    
    public FileTypeDvo getFileTypeDvo() {
        return this.fileTypeDvo;
    }
    
    // documentRoleDvo
    
    public void setDocumentRoleDvo(DocumentRoleDvo dvo) {
        this.documentRoleDvo = dvo;
    }
    
    public DocumentRoleDvo getDocumentRoleDvo() {
        return this.documentRoleDvo;
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
