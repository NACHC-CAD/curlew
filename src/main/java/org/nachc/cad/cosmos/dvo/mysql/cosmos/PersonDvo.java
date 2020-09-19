//
// Data Value Object (DVO) for person
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;

import org.yaorma.dvo.Dvo;

public class PersonDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "person";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "guid",
        "username",
        "fname",
        "lname",
        "display_name",
        "password",
        "salt",
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
        "username",
        "fname",
        "lname",
        "displayName",
        "password",
        "salt",
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
        "Username",
        "Fname",
        "Lname",
        "DisplayName",
        "Password",
        "Salt",
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
    
    private String username;
    
    private String fname;
    
    private String lname;
    
    private String displayName;
    
    private String password;
    
    private String salt;
    
    private String createdBy;
    
    private String createdDate;
    
    private String updatedBy;
    
    private String updatedDate;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private ArrayList<BlockDvo> blockCreatedByList = new ArrayList<BlockDvo>();
    
    private ArrayList<BlockDvo> blockUpdatedByList = new ArrayList<BlockDvo>();
    
    private ArrayList<BlockDefDvo> blockDefCreatedByList = new ArrayList<BlockDefDvo>();
    
    private ArrayList<BlockDefDvo> blockDefUpdatedByList = new ArrayList<BlockDefDvo>();
    
    private ArrayList<DocumentDvo> documentCreatedByList = new ArrayList<DocumentDvo>();
    
    private ArrayList<DocumentDvo> documentUpdatedByList = new ArrayList<DocumentDvo>();
    
    private ArrayList<DocumentDefDvo> documentDefCreatedByList = new ArrayList<DocumentDefDvo>();
    
    private ArrayList<DocumentDefDvo> documentDefUpdatedByList = new ArrayList<DocumentDefDvo>();
    
    private ArrayList<DocumentRoleDvo> documentRoleCreatedByList = new ArrayList<DocumentRoleDvo>();
    
    private ArrayList<DocumentRoleDvo> documentRoleUpdatedByList = new ArrayList<DocumentRoleDvo>();
    
    private ArrayList<DocumentValidatorDvo> documentValidatorCreatedByList = new ArrayList<DocumentValidatorDvo>();
    
    private ArrayList<DocumentValidatorDvo> documentValidatorUpdatedByList = new ArrayList<DocumentValidatorDvo>();
    
    private ArrayList<FileTypeDvo> fileTypeCreatedByList = new ArrayList<FileTypeDvo>();
    
    private ArrayList<FileTypeDvo> fileTypeUpdatedByList = new ArrayList<FileTypeDvo>();
    
    private ArrayList<PersonDvo> personCreatedByList = new ArrayList<PersonDvo>();
    
    private ArrayList<PersonDvo> personUpdatedByList = new ArrayList<PersonDvo>();
    
    private ArrayList<ProjectDvo> projectCreatedByList = new ArrayList<ProjectDvo>();
    
    private ArrayList<ProjectDvo> projectUpdatedByList = new ArrayList<ProjectDvo>();
    
    private ArrayList<StatusDvo> statusCreatedByList = new ArrayList<StatusDvo>();
    
    private ArrayList<StatusDvo> statusUpdatedByList = new ArrayList<StatusDvo>();
    
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
    
    // username
    
    public void setUsername(String str) {
        this.username = str;
    }
    
    public String getUsername() {
        return this.username;
    }
    
    // fname
    
    public void setFname(String str) {
        this.fname = str;
    }
    
    public String getFname() {
        return this.fname;
    }
    
    // lname
    
    public void setLname(String str) {
        this.lname = str;
    }
    
    public String getLname() {
        return this.lname;
    }
    
    // displayName
    
    public void setDisplayName(String str) {
        this.displayName = str;
    }
    
    public String getDisplayName() {
        return this.displayName;
    }
    
    // password
    
    public void setPassword(String str) {
        this.password = str;
    }
    
    public String getPassword() {
        return this.password;
    }
    
    // salt
    
    public void setSalt(String str) {
        this.salt = str;
    }
    
    public String getSalt() {
        return this.salt;
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
    
    public ArrayList<BlockDvo> getBlockCreatedByList() {
        return blockCreatedByList;
    }
    
    public void setBlockCreatedByList(ArrayList<BlockDvo> list) {
        this.blockCreatedByList = list;
    }
    
    public ArrayList<BlockDvo> getBlockUpdatedByList() {
        return blockUpdatedByList;
    }
    
    public void setBlockUpdatedByList(ArrayList<BlockDvo> list) {
        this.blockUpdatedByList = list;
    }
    
    public ArrayList<BlockDefDvo> getBlockDefCreatedByList() {
        return blockDefCreatedByList;
    }
    
    public void setBlockDefCreatedByList(ArrayList<BlockDefDvo> list) {
        this.blockDefCreatedByList = list;
    }
    
    public ArrayList<BlockDefDvo> getBlockDefUpdatedByList() {
        return blockDefUpdatedByList;
    }
    
    public void setBlockDefUpdatedByList(ArrayList<BlockDefDvo> list) {
        this.blockDefUpdatedByList = list;
    }
    
    public ArrayList<DocumentDvo> getDocumentCreatedByList() {
        return documentCreatedByList;
    }
    
    public void setDocumentCreatedByList(ArrayList<DocumentDvo> list) {
        this.documentCreatedByList = list;
    }
    
    public ArrayList<DocumentDvo> getDocumentUpdatedByList() {
        return documentUpdatedByList;
    }
    
    public void setDocumentUpdatedByList(ArrayList<DocumentDvo> list) {
        this.documentUpdatedByList = list;
    }
    
    public ArrayList<DocumentDefDvo> getDocumentDefCreatedByList() {
        return documentDefCreatedByList;
    }
    
    public void setDocumentDefCreatedByList(ArrayList<DocumentDefDvo> list) {
        this.documentDefCreatedByList = list;
    }
    
    public ArrayList<DocumentDefDvo> getDocumentDefUpdatedByList() {
        return documentDefUpdatedByList;
    }
    
    public void setDocumentDefUpdatedByList(ArrayList<DocumentDefDvo> list) {
        this.documentDefUpdatedByList = list;
    }
    
    public ArrayList<DocumentRoleDvo> getDocumentRoleCreatedByList() {
        return documentRoleCreatedByList;
    }
    
    public void setDocumentRoleCreatedByList(ArrayList<DocumentRoleDvo> list) {
        this.documentRoleCreatedByList = list;
    }
    
    public ArrayList<DocumentRoleDvo> getDocumentRoleUpdatedByList() {
        return documentRoleUpdatedByList;
    }
    
    public void setDocumentRoleUpdatedByList(ArrayList<DocumentRoleDvo> list) {
        this.documentRoleUpdatedByList = list;
    }
    
    public ArrayList<DocumentValidatorDvo> getDocumentValidatorCreatedByList() {
        return documentValidatorCreatedByList;
    }
    
    public void setDocumentValidatorCreatedByList(ArrayList<DocumentValidatorDvo> list) {
        this.documentValidatorCreatedByList = list;
    }
    
    public ArrayList<DocumentValidatorDvo> getDocumentValidatorUpdatedByList() {
        return documentValidatorUpdatedByList;
    }
    
    public void setDocumentValidatorUpdatedByList(ArrayList<DocumentValidatorDvo> list) {
        this.documentValidatorUpdatedByList = list;
    }
    
    public ArrayList<FileTypeDvo> getFileTypeCreatedByList() {
        return fileTypeCreatedByList;
    }
    
    public void setFileTypeCreatedByList(ArrayList<FileTypeDvo> list) {
        this.fileTypeCreatedByList = list;
    }
    
    public ArrayList<FileTypeDvo> getFileTypeUpdatedByList() {
        return fileTypeUpdatedByList;
    }
    
    public void setFileTypeUpdatedByList(ArrayList<FileTypeDvo> list) {
        this.fileTypeUpdatedByList = list;
    }
    
    public ArrayList<PersonDvo> getPersonCreatedByList() {
        return personCreatedByList;
    }
    
    public void setPersonCreatedByList(ArrayList<PersonDvo> list) {
        this.personCreatedByList = list;
    }
    
    public ArrayList<PersonDvo> getPersonUpdatedByList() {
        return personUpdatedByList;
    }
    
    public void setPersonUpdatedByList(ArrayList<PersonDvo> list) {
        this.personUpdatedByList = list;
    }
    
    public ArrayList<ProjectDvo> getProjectCreatedByList() {
        return projectCreatedByList;
    }
    
    public void setProjectCreatedByList(ArrayList<ProjectDvo> list) {
        this.projectCreatedByList = list;
    }
    
    public ArrayList<ProjectDvo> getProjectUpdatedByList() {
        return projectUpdatedByList;
    }
    
    public void setProjectUpdatedByList(ArrayList<ProjectDvo> list) {
        this.projectUpdatedByList = list;
    }
    
    public ArrayList<StatusDvo> getStatusCreatedByList() {
        return statusCreatedByList;
    }
    
    public void setStatusCreatedByList(ArrayList<StatusDvo> list) {
        this.statusCreatedByList = list;
    }
    
    public ArrayList<StatusDvo> getStatusUpdatedByList() {
        return statusUpdatedByList;
    }
    
    public void setStatusUpdatedByList(ArrayList<StatusDvo> list) {
        this.statusUpdatedByList = list;
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
