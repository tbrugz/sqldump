package tbrugz.sqldiff.compare;

import java.util.Comparator;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;

public class ExecOrderDiffComparator implements Comparator<Diff> {
	
	/*
	 * Compares its two arguments for order. Returns a negative integer, zero, 
	 * or a positive integer as the first argument is less than, equal to, or 
	 * greater than the second.
	 * 
	 */
	@Override
	public int compare(Diff o1, Diff o2) {
		int comp = o1.getChangeType().compareTo(o2.getChangeType());
		//if change types differs, compare with it
		if(comp!=0) {
			return -comp;
		}
		
		if(o1.getObjectType().equals(o2.getObjectType())) {
			//if same object type, compare by name
			return compare(o1.getNamedObject(), o2.getNamedObject()); //OK!
		}
		
		int mult = -1;
		if(o1.getChangeType().equals(ChangeType.ADD)) {
			//if change type is add, use direct object type order, otherwise use inverse order 
			mult = 1;
		}
		return (dbObjectAddOrder(o1.getObjectType()) - dbObjectAddOrder(o2.getObjectType()) ) * mult;
		//return o1.getObjectType().compareTo(o2.getObjectType()) * mult;
	}
	
	public static int compare(NamedDBObject o1, NamedDBObject o2) {
		int comp = o1.getSchemaName()!=null?
				o1.getSchemaName().compareTo(o2.getSchemaName()):
				o2.getSchemaName()!=null?1:0; //XXX: return -1? 1?
		if(comp!=0) return comp;
		return o1.getName().compareTo(o2.getName());
	}
	
	public static int dbObjectAddOrder(DBObjectType t) {
		switch (t) {
		case TABLE: return 1;
		case COLUMN: return 2;
		case SYNONYM: return 3;
		case SEQUENCE: return 4;
		case CONSTRAINT: return 5;
		case FK: return 6;
		
		case INDEX: return 10;
		case VIEW: return 11;
		case MATERIALIZED_VIEW: return 12;
		
		case TYPE: return 15;
		case TYPE_BODY: return 16;
		
		case FUNCTION: return 20;
		case PROCEDURE: return 21;
		case PACKAGE: return 22;
		case PACKAGE_BODY: return 23;
		case EXECUTABLE: return 24;
		
		case TRIGGER: return 30;
		case GRANT: return 31;

		//case JAVA_SOURCE: ?
		}
		return 100;
	}
}
