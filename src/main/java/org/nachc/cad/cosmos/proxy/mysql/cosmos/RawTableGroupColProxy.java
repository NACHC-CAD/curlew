package org.nachc.cad.cosmos.proxy.mysql.cosmos;

public class RawTableGroupColProxy {

	/**
	 * Returns the list of ALL columns that includes the newly added columns
	 * as well as the columns that existed before the insert.  
	 */
	
	/*
	public static List<RawTableGroupColDvo> addMissingCols(String rawTableGroupGuid, List<RawTableColDvo> newCols, List<RawTableGroupColDvo> existingCols, String createdByGuid, Connection conn) {
		List<RawTableGroupColDvo> missing = getMissingCols(rawTableGroupGuid, newCols, existingCols, createdByGuid);
		Dao.insert(missing, conn);
		List<RawTableGroupColDvo> all = Dao.findList(new RawTableGroupColDvo(), "raw_table_group", rawTableGroupGuid, "col_name", conn);
		return all;
	}
	
	public static List<RawTableGroupColDvo> getMissingCols(String rawTableGroupGuid, List<RawTableColDvo> newCols, List<RawTableGroupColDvo> existingCols, String createdByGuid) {
		List<RawTableGroupColDvo> rtn = new ArrayList<RawTableGroupColDvo>();
		for(RawTableColDvo newCol : newCols) {
			if(containsCol(newCol.getColName(), existingCols) == false) {
				RawTableGroupColDvo newColDvo = getNew(newCol, rawTableGroupGuid, createdByGuid);
				rtn.add(newColDvo);
			}
		}
		return rtn;
	}
	
	public static boolean containsCol(String colName, List<RawTableGroupColDvo> list) {
		for(RawTableGroupColDvo dvo : list) {
			if(colName.equals(dvo.getColName())) {
				return true;
			}
		}
		return false;
	}
	
	public static RawTableGroupColDvo getNew(RawTableColDvo newCol, String rawTableGroupGuid, String createdByGuid) {
		RawTableGroupColDvo rtn = new RawTableGroupColDvo();
		CosmosDvoUtil.init(rtn, createdByGuid);
		rtn.setRawTableGroup(rawTableGroupGuid);
		rtn.setColName(newCol.getColName());
		return rtn;
	}
	*/
	
}
