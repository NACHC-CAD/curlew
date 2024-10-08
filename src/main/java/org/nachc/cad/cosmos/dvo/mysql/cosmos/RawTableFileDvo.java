//
// Data Value Object (DVO) for raw_table_file
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class RawTableFileDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "raw_table_file";
    
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
        "data_lot",
        "file_location",
        "file_name",
        "file_size",
        "file_size_units",
        "guid",
        "org_code",
        "project",
        "provided_by",
        "provided_date",
        "raw_table",
        "updated_by",
        "updated_date"
    };
    
    //
    // primaryKeyColumnNames
    //
    
    public static final String[] PRIMARY_KEY_COLUMN_NAMES = {
        "raw_table"
    };
    
    //
    // javaNames
    //
    
    public static final String[] JAVA_NAMES = {
        "createdBy",
        "createdDate",
        "dataLot",
        "fileLocation",
        "fileName",
        "fileSize",
        "fileSizeUnits",
        "guid",
        "orgCode",
        "project",
        "providedBy",
        "providedDate",
        "rawTable",
        "updatedBy",
        "updatedDate"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "CreatedBy",
        "CreatedDate",
        "DataLot",
        "FileLocation",
        "FileName",
        "FileSize",
        "FileSizeUnits",
        "Guid",
        "OrgCode",
        "Project",
        "ProvidedBy",
        "ProvidedDate",
        "RawTable",
        "UpdatedBy",
        "UpdatedDate"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String createdBy;
    
    private Date createdDate;
    
    private String dataLot;
    
    private String fileLocation;
    
    private String fileName;
    
    private Long fileSize;
    
    private String fileSizeUnits;
    
    private String guid;
    
    private String orgCode;
    
    private String project;
    
    private String providedBy;
    
    private Date providedDate;
    
    private String rawTable;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private OrgCodeDvo orgDvo;
    
    private ProjCodeDvo projectDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
    private RawTableDvo rawTableDvo;
    
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
    
    // dataLot
    
    public void setDataLot(String val) {
        this.dataLot = val;
    }
    
    public String getDataLot() {
        return this.dataLot;
    }
    
    // fileLocation
    
    public void setFileLocation(String val) {
        this.fileLocation = val;
    }
    
    public String getFileLocation() {
        return this.fileLocation;
    }
    
    // fileName
    
    public void setFileName(String val) {
        this.fileName = val;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    // fileSize
    
    public void setFileSize(Long val) {
        this.fileSize = val;
    }
    
    public Long getFileSize() {
        return this.fileSize;
    }
    
    // fileSizeUnits
    
    public void setFileSizeUnits(String val) {
        this.fileSizeUnits = val;
    }
    
    public String getFileSizeUnits() {
        return this.fileSizeUnits;
    }
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // orgCode
    
    public void setOrgCode(String val) {
        this.orgCode = val;
    }
    
    public String getOrgCode() {
        return this.orgCode;
    }
    
    // project
    
    public void setProject(String val) {
        this.project = val;
    }
    
    public String getProject() {
        return this.project;
    }
    
    // providedBy
    
    public void setProvidedBy(String val) {
        this.providedBy = val;
    }
    
    public String getProvidedBy() {
        return this.providedBy;
    }
    
    // providedDate
    
    public void setProvidedDate(Date val) {
        this.providedDate = val;
    }
    
    public Date getProvidedDate() {
        return this.providedDate;
    }
    
    // rawTable
    
    public void setRawTable(String val) {
        this.rawTable = val;
    }
    
    public String getRawTable() {
        return this.rawTable;
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
    
    // orgDvo
    
    public void setOrgDvo(OrgCodeDvo dvo) {
        this.orgDvo = dvo;
    }
    
    public OrgCodeDvo getOrgDvo() {
        return this.orgDvo;
    }
    
    // projectDvo
    
    public void setProjectDvo(ProjCodeDvo dvo) {
        this.projectDvo = dvo;
    }
    
    public ProjCodeDvo getProjectDvo() {
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
    
    // rawTableDvo
    
    public void setRawTableDvo(RawTableDvo dvo) {
        this.rawTableDvo = dvo;
    }
    
    public RawTableDvo getRawTableDvo() {
        return this.rawTableDvo;
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
            getRawTable()
        };
        return rtn;
    }
}
