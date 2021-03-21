//
// Data Value Object (DVO) for proj_code
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class ProjCodeDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "proj_code";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "code",
        "name"
    };
    
    //
    // primaryKeyColumnNames
    //
    
    public static final String[] PRIMARY_KEY_COLUMN_NAMES = {
        "code"
    };
    
    //
    // javaNames
    //
    
    public static final String[] JAVA_NAMES = {
        "code",
        "name"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "Code",
        "Name"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String code;
    
    private String name;
    
    private ArrayList<ProjUrlDvo> projUrlProjectList = new ArrayList<ProjUrlDvo>();
    
    private ArrayList<RawTableGroupDvo> rawTableGroupProjectList = new ArrayList<RawTableGroupDvo>();
    
    private ArrayList<RawTableDvo> rawTableProjectList = new ArrayList<RawTableDvo>();
    
    private ArrayList<RawTableColDvo> rawTableColProjectList = new ArrayList<RawTableColDvo>();
    
    private ArrayList<RawTableFileDvo> rawTableFileProjectList = new ArrayList<RawTableFileDvo>();
    
    //
    // trivial getters and setters
    //
    
    // code
    
    public void setCode(String val) {
        this.code = val;
    }
    
    public String getCode() {
        return this.code;
    }
    
    // name
    
    public void setName(String val) {
        this.name = val;
    }
    
    public String getName() {
        return this.name;
    }
    
    public ArrayList<ProjUrlDvo> getProjUrlProjectList() {
        return projUrlProjectList;
    }
    
    public void setProjUrlProjectList(ArrayList<ProjUrlDvo> list) {
        this.projUrlProjectList = list;
    }
    
    public ArrayList<RawTableGroupDvo> getRawTableGroupProjectList() {
        return rawTableGroupProjectList;
    }
    
    public void setRawTableGroupProjectList(ArrayList<RawTableGroupDvo> list) {
        this.rawTableGroupProjectList = list;
    }
    
    public ArrayList<RawTableDvo> getRawTableProjectList() {
        return rawTableProjectList;
    }
    
    public void setRawTableProjectList(ArrayList<RawTableDvo> list) {
        this.rawTableProjectList = list;
    }
    
    public ArrayList<RawTableColDvo> getRawTableColProjectList() {
        return rawTableColProjectList;
    }
    
    public void setRawTableColProjectList(ArrayList<RawTableColDvo> list) {
        this.rawTableColProjectList = list;
    }
    
    public ArrayList<RawTableFileDvo> getRawTableFileProjectList() {
        return rawTableFileProjectList;
    }
    
    public void setRawTableFileProjectList(ArrayList<RawTableFileDvo> list) {
        this.rawTableFileProjectList = list;
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
            getCode()
        };
        return rtn;
    }
}
