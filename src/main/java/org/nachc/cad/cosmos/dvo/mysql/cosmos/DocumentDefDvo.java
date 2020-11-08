//
// Data Value Object (DVO) for document_def
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

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
        "row_id",
        "block_def",
        "file_type",
        "document_role",
        "data_group",
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
        "rowId",
        "blockDef",
        "fileType",
        "documentRole",
        "dataGroup",
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
        "RowId",
        "BlockDef",
        "FileType",
        "DocumentRole",
        "DataGroup",
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
    
    private Integer rowId;
    
    private String blockDef;
    
    private String fileType;
    
    private String documentRole;
    
    private String dataGroup;
    
    private String name;
    
    private String description;
    
    private String validator;
    
    private String databricksDir;
    
    private String createdBy;
    
    private Date createdDate;
    
    private String updatedBy;
    
    private Date updatedDate;
    
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
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // rowId
    
    public void setRowId(Integer val) {
        this.rowId = val;
    }
    
    public Integer getRowId() {
        return this.rowId;
    }
    
    // blockDef
    
    public void setBlockDef(String val) {
        this.blockDef = val;
    }
    
    public String getBlockDef() {
        return this.blockDef;
    }
    
    // fileType
    
    public void setFileType(String val) {
        this.fileType = val;
    }
    
    public String getFileType() {
        return this.fileType;
    }
    
    // documentRole
    
    public void setDocumentRole(String val) {
        this.documentRole = val;
    }
    
    public String getDocumentRole() {
        return this.documentRole;
    }
    
    // dataGroup
    
    public void setDataGroup(String val) {
        this.dataGroup = val;
    }
    
    public String getDataGroup() {
        return this.dataGroup;
    }
    
    // name
    
    public void setName(String val) {
        this.name = val;
    }
    
    public String getName() {
        return this.name;
    }
    
    // description
    
    public void setDescription(String val) {
        this.description = val;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    // validator
    
    public void setValidator(String val) {
        this.validator = val;
    }
    
    public String getValidator() {
        return this.validator;
    }
    
    // databricksDir
    
    public void setDatabricksDir(String val) {
        this.databricksDir = val;
    }
    
    public String getDatabricksDir() {
        return this.databricksDir;
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
