//
// Data Value Object (DVO) for document_def
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;

import org.yaorma.dvo.Dvo;

public class DocumentDefDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "document_def";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "guid",
        "block_def",
        "file_type",
        "document_role",
        "row_id",
        "name",
        "description",
        "validator",
        "databricks_dir",
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
        "blockDef",
        "fileType",
        "documentRole",
        "rowId",
        "name",
        "description",
        "validator",
        "databricksDir",
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
        "BlockDef",
        "FileType",
        "DocumentRole",
        "RowId",
        "Name",
        "Description",
        "Validator",
        "DatabricksDir",
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
    
    private String blockDef;
    
    private String fileType;
    
    private String documentRole;
    
    private String rowId;
    
    private String name;
    
    private String description;
    
    private String validator;
    
    private String databricksDir;
    
    private String createdBy;
    
    private String createdDate;
    
    private String updatedBy;
    
    private String updatedDate;
    
    private BlockDefDvo blockDefDvo;
    
    private FileTypeDvo fileTypeDvo;
    
    private DocumentRoleDvo documentRoleDvo;
    
    private DocumentValidatorDvo validatorDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<DocumentDvo> documentDocumentDefList = new ArrayList<DocumentDvo>();
    
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
    
    // blockDef
    
    public void setBlockDef(String str) {
        this.blockDef = str;
    }
    
    public String getBlockDef() {
        return this.blockDef;
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
    
    // rowId
    
    public void setRowId(String str) {
        this.rowId = str;
    }
    
    public String getRowId() {
        return this.rowId;
    }
    
    // name
    
    public void setName(String str) {
        this.name = str;
    }
    
    public String getName() {
        return this.name;
    }
    
    // description
    
    public void setDescription(String str) {
        this.description = str;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    // validator
    
    public void setValidator(String str) {
        this.validator = str;
    }
    
    public String getValidator() {
        return this.validator;
    }
    
    // databricksDir
    
    public void setDatabricksDir(String str) {
        this.databricksDir = str;
    }
    
    public String getDatabricksDir() {
        return this.databricksDir;
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
    
    // validatorDvo
    
    public void setValidatorDvo(DocumentValidatorDvo dvo) {
        this.validatorDvo = dvo;
    }
    
    public DocumentValidatorDvo getValidatorDvo() {
        return this.validatorDvo;
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
    
    public ArrayList<DocumentDvo> getDocumentDocumentDefList() {
        return documentDocumentDefList;
    }
    
    public void setDocumentDocumentDefList(ArrayList<DocumentDvo> list) {
        this.documentDocumentDefList = list;
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
