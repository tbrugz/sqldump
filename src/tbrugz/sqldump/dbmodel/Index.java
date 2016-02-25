package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * see: http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/statements_5010.htm
 * XXX: index type. e.g. bitmap (done for oracle)
 */
public class Index extends DBObject {
	private static final long serialVersionUID = 1L;
	
	public static class ByTableNameComparator implements Comparator<Index> {
		@Override
		public int compare(Index o1, Index o2) {
			int compare = 0;
			if( (o1.schemaName!=null && o2.schemaName!=null)
				|| (o1.schemaName==null && o2.schemaName==null) ) {
				compare = o1.tableName.compareTo(o2.tableName);
			}
			else if(o1.schemaName==null || o2.schemaName==null) {
				compare = o1.schemaName!=null?-1:+1;
			}
			if(compare!=0) { return compare; }
			compare = o1.compareTo(o2);
			
			return compare;
		}
	}
	
	public enum IndexType {
		NORMAL,
		FUNCTION_BASED_NORMAL
	}
	
	boolean unique;
	String type;
	IndexType indexType;
	Boolean reverse;
	String tableName; //XXX: Table instead of tableName?
	final List<String> columns = new ArrayList<String>();
	String comment;
	Boolean local;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create "+(unique?"unique ":"")+(type!=null?type.toLowerCase()+" ":"")+"index "+getFinalName(dumpSchemaName)
			+" on "+DBObject.getFinalName(getSchemaName(), tableName, dumpSchemaName)
			+" ("+Utils.join(columns, ", ", SQLIdentifierDecorator.getInstance())+")"
			+((local!=null && local)?" local":"")
			+(reverse!=null&&reverse?" reverse":"")+(comment!=null?" /* "+comment+" */":"");
	}
	
	@Override
	public String toString() {
		return "[Index:"+(getSchemaName()!=null?getSchemaName()+".":"")+getName()+":t:"+tableName+",u?:"+unique+",c:"+columns+"]";
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
		if (obj == null)
			return false;
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
	public int compareTo(DBIdentifiable o) {
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

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Boolean getReverse() {
		return reverse;
	}

	public void setReverse(Boolean reverse) {
		this.reverse = reverse;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public Boolean getLocal() {
		return local;
	}

	public void setLocal(Boolean local) {
		this.local = local;
	}

	public List<String> getColumns() {
		return columns;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public void setIndexType(IndexType indexType) {
		this.indexType = indexType;
	}

}
