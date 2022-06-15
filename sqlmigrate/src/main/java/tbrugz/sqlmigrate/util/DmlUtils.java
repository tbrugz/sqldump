package tbrugz.sqlmigrate.util;

import java.util.List;

public class DmlUtils {

	public static String createInsert(String schemaName, String tableName, List<String> columnNames) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into "+(schemaName!=null?schemaName+".":"")+tableName+" (");
		for(int i=0;i<columnNames.size();i++) {
			if(i>0) { sb.append(", "); }
			sb.append(columnNames.get(i));
		}
		sb.append(") values (");
		for(int i=0;i<columnNames.size();i++) {
			if(i>0) { sb.append(", "); }
			sb.append("?");
		}
		sb.append(")");
		return sb.toString();
	}

	public static String createDelete(String schemaName, String tableName, List<String> filterColumnNames) {
		StringBuilder sb = new StringBuilder();
		sb.append("delete from "+(schemaName!=null?schemaName+".":"")+tableName);
		//if(columnNames.size()>0) {
		sb.append(" where ");
		for(int i=0;i<filterColumnNames.size();i++) {
			if(i>0) { sb.append(" and "); }
			sb.append(filterColumnNames.get(i) + " = ?");
		}
		//}
		return sb.toString();
	}
	
}
