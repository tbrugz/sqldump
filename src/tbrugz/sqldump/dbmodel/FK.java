package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

//XXX~: extends DBObject?
//XXX: should be constraint?
public class FK extends DBIdentifiable implements Comparable<FK>, Serializable {
	
	public static enum UpdateRule {
		NO_ACTION,
		CASCADE,
		SET_NULL,
		SET_DEFAULT;
		
		public String toString() {
			switch (this) {
			case NO_ACTION:
				return null;
			case CASCADE:
				return "cascade";
			case SET_NULL:
				return "set null";
			case SET_DEFAULT:
				return "set null";
			}
			return "unknown";
		};
		
		public static UpdateRule getUpdateRule(String s) {
			if(s==null) return null;
			if("NO ACTION".equals(s)) return null; //XXX return NO_ACTION; ?
			if("CASCADE".equals(s)) return CASCADE;
			if("SET NULL".equals(s)) return SET_NULL;
			if("SET DEFAULT".equals(s)) return SET_DEFAULT;
			return null;
		}
	}
	
	private static final long serialVersionUID = 1L;
	//String name;
	public String pkTable;
	public String fkTable;
	public String pkTableSchemaName;
	public String fkTableSchemaName;
	public Boolean fkReferencesPK; //FK references a PK? true. references a UK (unique key)? false
	public UpdateRule updateRule;
	public UpdateRule deleteRule;

	public List<String> pkColumns = new ArrayList<String>();
	public List<String> fkColumns = new ArrayList<String>();
	
	@Override
	public String toString() {
		return name+"["+fkTable+"<-"+pkTable+"]";
		//return "fk:"+name+"["+fkTable+"<-"+pkTable+"]";
	}

	public String toStringFull() {
		return name+"["+fkTable+"("+fkColumns+")"+"<-"+pkTable+"("+pkColumns+")"+"]";
	}
	
	/*public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}*/

	public int compareTo(FK o) {
		int fkCompare = fkTable.compareTo(o.fkTable);
		if(fkCompare==0) { //if same FK Table, compare FK Name
			return name.compareTo(o.name);
		}
		return fkCompare;
		// return name.compareTo(o.name);
	}

	public String fkSimpleScript(String whitespace, boolean dumpWithSchemaName) {
		whitespace = whitespace.replaceAll("[^ \n\t]", " ");
		return "constraint "+DBObject.getFinalIdentifier(getName())
			+" foreign key ("+Utils.join(fkColumns, ", ", SQLIdentifierDecorator.getInstance())+")"
			+whitespace+"references "+(dumpWithSchemaName?DBObject.getFinalIdentifier(pkTableSchemaName)+".":"")
			+DBObject.getFinalIdentifier(pkTable)+" ("+Utils.join(pkColumns, ", ", SQLIdentifierDecorator.getInstance())+")"
			+(updateRule!=null && updateRule!=UpdateRule.NO_ACTION?" on update "+updateRule:"")
			+(deleteRule!=null && deleteRule!=UpdateRule.NO_ACTION?" on delete "+deleteRule:"")
			;
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return fkSimpleScript(" ", true);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FK) {
			FK fk = (FK) obj;
			return pkTable.equalsIgnoreCase(fk.pkTable) && fkTable.equalsIgnoreCase(fk.fkTable) 
					&& Utils.stringListEqualIgnoreCase(pkColumns, fk.pkColumns) && Utils.stringListEqualIgnoreCase(fkColumns, fk.fkColumns);
		}
		return false;
	}
	
	@Override
	public String getSchemaName() {
		return fkTableSchemaName;
	}
}
