//
// Data Value Object (DVO) for person
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

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
        "created_by",
        "created_date",
        "display_name",
        "fname",
        "guid",
        "lname",
        "password",
        "salt",
        "updated_by",
        "updated_date",
        "username"
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
        "createdBy",
        "createdDate",
        "displayName",
        "fname",
        "guid",
        "lname",
        "password",
        "salt",
        "updatedBy",
        "updatedDate",
        "username"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "CreatedBy",
        "CreatedDate",
        "DisplayName",
        "Fname",
        "Guid",
        "Lname",
        "Password",
        "Salt",
        "UpdatedBy",
        "UpdatedDate",
        "Username"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String createdBy;
    
    private Date createdDate;
    
    private String displayName;
    
    private String fname;
    
    private String guid;
    
    private String lname;
    
    private String password;
    
    private String salt;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private String username;
    
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
    
    private ArrayList<ProjUrlDvo> projUrlCreatedByList = new ArrayList<ProjUrlDvo>();
    
    private ArrayList<ProjUrlDvo> projUrlUpdatedByList = new ArrayList<ProjUrlDvo>();
    
    private ArrayList<ProjectDvo> projectCreatedByList = new ArrayList<ProjectDvo>();
    
    private ArrayList<ProjectDvo> projectUpdatedByList = new ArrayList<ProjectDvo>();
    
    private ArrayList<RawTableDvo> rawTableCreatedByList = new ArrayList<RawTableDvo>();
    
    private ArrayList<RawTableDvo> rawTableUpdatedByList = new ArrayList<RawTableDvo>();
    
    private ArrayList<RawTableColDvo> rawTableColCreatedByList = new ArrayList<RawTableColDvo>();
    
    private ArrayList<RawTableColDvo> rawTableColUpdatedByList = new ArrayList<RawTableColDvo>();
    
    private ArrayList<RawTableFileDvo> rawTableFileCreatedByList = new ArrayList<RawTableFileDvo>();
    
    private ArrayList<RawTableFileDvo> rawTableFileUpdatedByList = new ArrayList<RawTableFileDvo>();
    
    private ArrayList<RawTableGroupDvo> rawTableGroupCreatedByList = new ArrayList<RawTableGroupDvo>();
    
    private ArrayList<RawTableGroupDvo> rawTableGroupUpdatedByList = new ArrayList<RawTableGroupDvo>();
    
    private ArrayList<StatusDvo> statusCreatedByList = new ArrayList<StatusDvo>();
    
    private ArrayList<StatusDvo> statusUpdatedByList = new ArrayList<StatusDvo>();
    
    //
    // trivial getters and setters
    //
    
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
    
    // displayName
    
    public void setDisplayName(String val) {
        this.displayName = val;
    }
    
    public String getDisplayName() {
        return this.displayName;
    }
    
    // fname
    
    public void setFname(String val) {
        this.fname = val;
    }
    
    public String getFname() {
        return this.fname;
    }
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // lname
    
    public void setLname(String val) {
        this.lname = val;
    }
    
    public String getLname() {
        return this.lname;
    }
    
    // password
    
    public void setPassword(String val) {
        this.password = val;
    }
    
    public String getPassword() {
        return this.password;
    }
    
    // salt
    
    public void setSalt(String val) {
        this.salt = val;
    }
    
    public String getSalt() {
        return this.salt;
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
    
    // username
    
    public void setUsername(String val) {
        this.username = val;
    }
    
    public String getUsername() {
        return this.username;
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
    
    public ArrayList<ProjUrlDvo> getProjUrlCreatedByList() {
        return projUrlCreatedByList;
    }
    
    public void setProjUrlCreatedByList(ArrayList<ProjUrlDvo> list) {
        this.projUrlCreatedByList = list;
    }
    
    public ArrayList<ProjUrlDvo> getProjUrlUpdatedByList() {
        return projUrlUpdatedByList;
    }
    
    public void setProjUrlUpdatedByList(ArrayList<ProjUrlDvo> list) {
        this.projUrlUpdatedByList = list;
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
    
    public ArrayList<RawTableDvo> getRawTableCreatedByList() {
        return rawTableCreatedByList;
    }
    
    public void setRawTableCreatedByList(ArrayList<RawTableDvo> list) {
        this.rawTableCreatedByList = list;
    }
    
    public ArrayList<RawTableDvo> getRawTableUpdatedByList() {
        return rawTableUpdatedByList;
    }
    
    public void setRawTableUpdatedByList(ArrayList<RawTableDvo> list) {
        this.rawTableUpdatedByList = list;
    }
    
    public ArrayList<RawTableColDvo> getRawTableColCreatedByList() {
        return rawTableColCreatedByList;
    }
    
    public void setRawTableColCreatedByList(ArrayList<RawTableColDvo> list) {
        this.rawTableColCreatedByList = list;
    }
    
    public ArrayList<RawTableColDvo> getRawTableColUpdatedByList() {
        return rawTableColUpdatedByList;
    }
    
    public void setRawTableColUpdatedByList(ArrayList<RawTableColDvo> list) {
        this.rawTableColUpdatedByList = list;
    }
    
    public ArrayList<RawTableFileDvo> getRawTableFileCreatedByList() {
        return rawTableFileCreatedByList;
    }
    
    public void setRawTableFileCreatedByList(ArrayList<RawTableFileDvo> list) {
        this.rawTableFileCreatedByList = list;
    }
    
    public ArrayList<RawTableFileDvo> getRawTableFileUpdatedByList() {
        return rawTableFileUpdatedByList;
    }
    
    public void setRawTableFileUpdatedByList(ArrayList<RawTableFileDvo> list) {
        this.rawTableFileUpdatedByList = list;
    }
    
    public ArrayList<RawTableGroupDvo> getRawTableGroupCreatedByList() {
        return rawTableGroupCreatedByList;
    }
    
    public void setRawTableGroupCreatedByList(ArrayList<RawTableGroupDvo> list) {
        this.rawTableGroupCreatedByList = list;
    }
    
    public ArrayList<RawTableGroupDvo> getRawTableGroupUpdatedByList() {
        return rawTableGroupUpdatedByList;
    }
    
    public void setRawTableGroupUpdatedByList(ArrayList<RawTableGroupDvo> list) {
        this.rawTableGroupUpdatedByList = list;
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
