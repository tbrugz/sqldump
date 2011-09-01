package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.Utils;

/*
 * see: http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/statements_5010.htm
 * XXX: index type. e.g. bitmap (done for oracle)
 */
public class Index extends DBObject {
	
	public boolean unique;
	public String type;
	public Boolean reverse;
	public String tableName;
	public List<String> columns = new ArrayList<String>();
	public String comment;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create "+(unique?"unique ":"")+(type!=null?type+" ":"")+"index "+(dumpSchemaName?schemaName+".":"")+name+" on "+(dumpSchemaName?schemaName+".":"")+tableName
			+" ("+Utils.join(columns, ", ")+")"
			+(reverse!=null&&reverse?" reverse":"")+(comment!=null?" --"+comment:"");
	}
	
	@Override
	public String toString() {
		return "[Index:"+schemaName+"."+name+":t:"+tableName+",u?"+unique+",c:"+columns+"]";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Index other = (Index) obj;
		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (!columns.equals(other.columns))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (unique != other.unique)
			return false;
		return true;
	}

}
