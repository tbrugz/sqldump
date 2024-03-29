package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

//XXX~: extends DBObject?
public class FK extends AbstractConstraint implements Serializable {
	
	public static enum UpdateRule {
		NO_ACTION,
		CASCADE,
		SET_NULL,
		SET_DEFAULT;
		
		public String toString() {
			switch (this) {
			case NO_ACTION:
				return null; // return ""; //?
			case CASCADE:
				return "cascade";
			case SET_NULL:
				return "set null";
			case SET_DEFAULT:
				return "set default";
			default:
				break;
			}
			return "unknown";
		};
		
		/*public static UpdateRule getUpdateRule(String s) {
			if(s==null) return null;
			if("NO ACTION".equals(s)) return null; //XXX return NO_ACTION; ?
			if("CASCADE".equals(s)) return CASCADE;
			if("SET NULL".equals(s)) return SET_NULL;
			if("SET DEFAULT".equals(s)) return SET_DEFAULT;
			return null;
		}*/

		public static UpdateRule getUpdateRule(Integer i) {
			if(i==null) { return null; }
			switch (i) {
			case DatabaseMetaData.importedKeyNoAction:
			case DatabaseMetaData.importedKeyRestrict:
				return null;
			case DatabaseMetaData.importedKeyCascade:
				return CASCADE;
			case DatabaseMetaData.importedKeySetNull:
				return SET_NULL;
			case DatabaseMetaData.importedKeySetDefault:
				return SET_DEFAULT;
			/* //used for "DEFERRABILITY":
			case DatabaseMetaData.importedKeyInitiallyDeferred:
			case DatabaseMetaData.importedKeyInitiallyImmediate:
			case DatabaseMetaData.importedKeyNotDeferrable: */
			default:
				break;
			}
			return null;
		}
	}
	
	private static final long serialVersionUID = 1L;
	//String name;
	String pkTable;
	String fkTable;
	String pkTableSchemaName;
	//String fkTableSchemaName; // getFkTableSchemaName() references DBIdentifiable.schemaName!
	Boolean fkReferencesPK; //FK references a PK? true. references a UK (unique key)? false
	UpdateRule updateRule;
	UpdateRule deleteRule;

	List<String> pkColumns = new ArrayList<String>();
	List<String> fkColumns = new ArrayList<String>();
	
	@Override
	public String toString() {
		return (schemaName!=null?schemaName+".":"")+name+"["+fkTable+">-"+pkTable+"]";
		//return "fk:"+name+"["+fkTable+"<-"+pkTable+"]";
	}

	public String toStringFull() {
		return (schemaName!=null?schemaName+".":"")+name+"["+fkTable+"("+fkColumns+")"+">-"+pkTable+"("+pkColumns+")"+"]";
	}
	
	/*public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}*/

	public int compareTo(DBIdentifiable o) {
		if(! (o instanceof FK)) { return super.compareTo(o); }
		FK ofk = (FK) o;
		
		int fkCompare = fkTable.compareTo(ofk.fkTable);
		if(fkCompare==0) { //if same FK Table, compare FK Name
			fkCompare = getName()!=null?getName().compareTo(o.getName()):o.getName()!=null?1:0;
			//return name.compareTo(o.name);
		}
		if(fkCompare==0) {
			fkCompare = getUpdateRule()!=null?getUpdateRule().compareTo(ofk.getUpdateRule()):ofk.getUpdateRule()!=null?1:0;
		}
		if(fkCompare==0) {
			fkCompare = getDeleteRule()!=null?getDeleteRule().compareTo(ofk.getDeleteRule()):ofk.getDeleteRule()!=null?1:0;
		}
		return fkCompare;
		// return name.compareTo(o.name);
	}

	public String fkSimpleScript(String whitespace, boolean dumpWithSchemaName) {
		whitespace = whitespace.replaceAll("[^ \n\t]", " ");
		String fkName = getName();
		return
			(fkName!=null ? "constraint "+DBObject.getFinalIdentifier(fkName)+" " : "")
			+"foreign key ("+Utils.join(fkColumns, ", ", SQLIdentifierDecorator.getInstance())+")"
			+whitespace+"references "
			+DBObject.getFinalName(pkTableSchemaName, pkTable, dumpWithSchemaName)
			+" ("+Utils.join(pkColumns, ", ", SQLIdentifierDecorator.getInstance())+")"
			+(updateRule!=null && updateRule!=UpdateRule.NO_ACTION?" on update "+updateRule:"")
			+(deleteRule!=null && deleteRule!=UpdateRule.NO_ACTION?" on delete "+deleteRule:"");
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return fkSimpleScript(" ", true);
	}
	
	public String getPkTable() {
		return pkTable;
	}

	public void setPkTable(String pkTable) {
		this.pkTable = pkTable;
	}

	public String getFkTable() {
		return fkTable;
	}

	public void setFkTable(String fkTable) {
		this.fkTable = fkTable;
	}

	public String getPkTableSchemaName() {
		return pkTableSchemaName;
	}

	public void setPkTableSchemaName(String pkTableSchemaName) {
		this.pkTableSchemaName = pkTableSchemaName;
	}

	public String getFkTableSchemaName() {
		return schemaName;
	}

	public void setFkTableSchemaName(String fkTableSchemaName) {
		this.schemaName = fkTableSchemaName;
	}

	public List<String> getPkColumns() {
		return pkColumns;
	}

	public void setPkColumns(List<String> pkColumns) {
		this.pkColumns = pkColumns;
	}

	public List<String> getFkColumns() {
		return fkColumns;
	}

	public void setFkColumns(List<String> fkColumns) {
		this.fkColumns = fkColumns;
	}

	public Boolean getFkReferencesPK() {
		return fkReferencesPK;
	}

	public void setFkReferencesPK(Boolean fkReferencesPK) {
		this.fkReferencesPK = fkReferencesPK;
	}

	public UpdateRule getUpdateRule() {
		return updateRule;
	}

	public void setUpdateRule(UpdateRule updateRule) {
		this.updateRule = updateRule;
	}

	public UpdateRule getDeleteRule() {
		return deleteRule;
	}

	public void setDeleteRule(UpdateRule deleteRule) {
		this.deleteRule = deleteRule;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FK) {
			FK fk = (FK) obj;
			return pkTable.equalsIgnoreCase(fk.pkTable)
					&& fkTable.equalsIgnoreCase(fk.fkTable) 
					&& Utils.stringListEqualIgnoreCase(pkColumns, fk.pkColumns)
					&& Utils.stringListEqualIgnoreCase(fkColumns, fk.fkColumns)
					&& getUpdateRule()!=null ? getUpdateRule().equals(fk.getUpdateRule()) : (fk.getUpdateRule()!=null?false:true)
					&& getDeleteRule()!=null ? getDeleteRule().equals(fk.getDeleteRule()) : (fk.getDeleteRule()!=null?false:true)
					;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((pkTable == null) ? 0 : pkTable.hashCode());
		result = prime * result + ((fkTable == null) ? 0 : fkTable.hashCode());
		result = prime * result + ((pkColumns == null) ? 0 : pkColumns.hashCode());
		result = prime * result + ((fkColumns == null) ? 0 : fkColumns.hashCode());
		return result;
	}
	
	/*@Override
	public String getSchemaName() {
		return schemaName;
	}*/

	//XXX: remove dumpDropStatements?
	public String fkScriptWithAlterTable(boolean dumpDropStatements, boolean dumpWithSchemaName) {
		String fkTableName = DBObject.getFinalName(getFkTableSchemaName(), getFkTable(), dumpWithSchemaName);
		// mysql: the "-- " (double-dash) comment style requires the second dash to be followed by at least one whitespace or control character
		return
			(dumpDropStatements?"-- alter table "+fkTableName+" drop constraint "+getName()+"\n":"")
			+"alter table "+fkTableName
			+"\n\tadd "+fkSimpleScript("\n\t", dumpWithSchemaName)+";\n";
	}
}
