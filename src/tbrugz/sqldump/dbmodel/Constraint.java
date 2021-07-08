package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

public class Constraint extends AbstractConstraint implements Serializable {

	private static final long serialVersionUID = 1L;

	public static enum ConstraintType {
		PK,     //CONSTRAINT pk1 PRIMARY KEY ("MANAGER_NAME", "EMAIL");
		UNIQUE, //CONSTRAINT unique1 UNIQUE ("MANAGER_NAME", "EMAIL");
		CHECK;  //CONSTRAINT check1 CHECK (char_length("EMAIL") > 5);
		//XXX: new class for check constraint?
		
		public String fullName() {
			switch (this) {
				case PK:
					return "primary key";
				case UNIQUE:
					return "unique";
				case CHECK:
					return "check";
				default:
					return this.toString();
			}
		}
	}

	ConstraintType type;
	//public String name;
	String checkDescription;
	List<String> uniqueColumns = new ArrayList<String>();
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		switch (type) {
			case CHECK:
				return "constraint "+DBObject.getFinalIdentifier(name)+" "+type.fullName()+" ("+checkDescription+")";
			case UNIQUE:
			case PK:
				return "constraint "+DBObject.getFinalIdentifier(name)+" "+type.fullName()+" ("+Utils.join(uniqueColumns, ", ", SQLIdentifierDecorator.getInstance())+")";
			default:
				break;
		}
		throw new RuntimeException("unknown constraint type: "+type);
	}
	
	@Override
	public int compareTo(DBIdentifiable o) {
		if(!(o instanceof Constraint)) {
			return super.compareTo(o);
		}
		
		Constraint c = (Constraint) o;
		if(type.equals(c.type)) {
			return name.compareTo(c.name);
		}
		return type.compareTo(c.type);
	}
	
	@Override
	public String toString() {
		//return getDefinition(false);
		return "["+type+":"+name+":"
				+(type==ConstraintType.CHECK?checkDescription:Utils.join(uniqueColumns, ","))
				+"]";
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Constraint) {
			Constraint cc = (Constraint) obj;
			if(!name.equalsIgnoreCase(cc.name)) { return false; }
			if(type.equals(cc.type)) {
				switch (type) {
				case CHECK:
					return checkDescription.equalsIgnoreCase(cc.checkDescription);
				case UNIQUE:
				case PK:
					//return uniqueColumns.equals(cc.uniqueColumns);
					return Utils.stringListEqualIgnoreCase(uniqueColumns, cc.uniqueColumns);
				default:
					break;
				}
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((uniqueColumns == null) ? 0 : uniqueColumns.hashCode());
		return result;
	}

	public ConstraintType getType() {
		return type;
	}

	public void setType(ConstraintType type) {
		this.type = type;
	}

	public String getCheckDescription() {
		return checkDescription;
	}

	public void setCheckDescription(String checkDescription) {
		this.checkDescription = checkDescription;
	}

	public List<String> getUniqueColumns() {
		return uniqueColumns;
	}

	public void setUniqueColumns(List<String> uniqueColumns) {
		this.uniqueColumns = uniqueColumns;
	}
	
}
