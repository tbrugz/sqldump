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
	public Boolean local;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create "+(unique?"unique ":"")+(type!=null?type+" ":"")+"index "+(dumpSchemaName?schemaName+".":"")+name+" on "+(dumpSchemaName?schemaName+".":"")+tableName
			+" ("+Utils.join(columns, ", ")+")"
			+((local!=null && local)?" local":"")
			+(reverse!=null&&reverse?" reverse":"")+(comment!=null?" --"+comment:"");
	}
	
	@Override
	public String toString() {
		return "[Index:"+schemaName+"."+name+":t:"+tableName+",u?:"+unique+",c:"+columns+"]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 7; //super.hashCode(); // Index equality will not depend on Index name
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
		result = prime * result + ((reverse == null) ? 0 : reverse.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + (unique ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		// Index equality will not depend on Index name
		//if (!super.equals(obj))
		//	return false;
		if (getClass() != obj.getClass())
			return false;
		Index other = (Index) obj;
		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (!columns.equals(other.columns))
			return false;
		if (reverse == null) {
			if (other.reverse != null)
				return false;
		} else if (!reverse.equals(other.reverse))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (unique != other.unique)
			return false;
		return true;
	}
	
	@Override
	public int compareTo(DBObject o) {
		int comp = super.compareTo(o);
		if(comp==0) {
			if(o instanceof Index) {
				comp = tableName.compareTo(((Index)o).tableName);
				if(comp==0) {
					comp = Utils.join(columns, "").compareTo(Utils.join(((Index)o).columns, ""));
				}
			}
			else {
				return -1; //???
			}
		}
		return comp;
	}

}
