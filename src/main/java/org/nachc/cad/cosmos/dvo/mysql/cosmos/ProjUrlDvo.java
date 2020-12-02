//
// Data Value Object (DVO) for proj_url
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class ProjUrlDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "proj_url";
    
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
        "guid",
        "link_text",
        "project",
        "sort_order",
        "updated_by",
        "updated_date",
        "url",
        "url_description",
        "url_type"
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
        "guid",
        "linkText",
        "project",
        "sortOrder",
        "updatedBy",
        "updatedDate",
        "url",
        "urlDescription",
        "urlType"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "CreatedBy",
        "CreatedDate",
        "Guid",
        "LinkText",
        "Project",
        "SortOrder",
        "UpdatedBy",
        "UpdatedDate",
        "Url",
        "UrlDescription",
        "UrlType"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String createdBy;
    
    private Date createdDate;
    
    private String guid;
    
    private String linkText;
    
    private String project;
    
    private Integer sortOrder;
    
    private String updatedBy;
    
    private Date updatedDate;
    
    private String url;
    
    private String urlDescription;
    
    private String urlType;
    
    private ProjCodeDvo projectDvo;
    
    private PersonDvo createdByDvo;
    
    private PersonDvo updatedByDvo;
    
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
    
    // guid
    
    public void setGuid(String val) {
        this.guid = val;
    }
    
    public String getGuid() {
        return this.guid;
    }
    
    // linkText
    
    public void setLinkText(String val) {
        this.linkText = val;
    }
    
    public String getLinkText() {
        return this.linkText;
    }
    
    // project
    
    public void setProject(String val) {
        this.project = val;
    }
    
    public String getProject() {
        return this.project;
    }
    
    // sortOrder
    
    public void setSortOrder(Integer val) {
        this.sortOrder = val;
    }
    
    public Integer getSortOrder() {
        return this.sortOrder;
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
    
    // url
    
    public void setUrl(String val) {
        this.url = val;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    // urlDescription
    
    public void setUrlDescription(String val) {
        this.urlDescription = val;
    }
    
    public String getUrlDescription() {
        return this.urlDescription;
    }
    
    // urlType
    
    public void setUrlType(String val) {
        this.urlType = val;
    }
    
    public String getUrlType() {
        return this.urlType;
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
