package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;

public class ModelUtils {

	public static Collection<? extends DBIdentifiable> getCollectionByType(SchemaModel model, DBObjectType type) {
		switch (type) {
		case TABLE:
			return model.getTables();
		case EXECUTABLE:
		case FUNCTION:
		case PACKAGE:
		case PACKAGE_BODY:
		case PROCEDURE:
		case TYPE:
		case TYPE_BODY:
			return model.getExecutables();
		case FK:
			return model.getForeignKeys();
		case MATERIALIZED_VIEW:
		case VIEW:
			return model.getViews();
		case TRIGGER:
			return model.getTriggers();
		case INDEX:
			return model.getIndexes();
		case SEQUENCE:
			return model.getSequences();
		case SYNONYM:
			return model.getSynonyms();
		default:
			break;
		}
		throw new IllegalArgumentException("Invalid object type: "+type);
	}
	
	public static List<FK> getImportedKeys(Relation rel, Set<FK> allFKs) {
		List<FK> fks = new ArrayList<FK>();
		for(FK fk: allFKs) {
			if( (rel.getSchemaName()==null || fk.fkTableSchemaName==null || rel.getSchemaName().equals(fk.fkTableSchemaName)) 
					&& rel.getName().equals(fk.fkTable)) {
				fks.add(fk);
			}
		}
		return fks;
	}

	public static List<FK> getExportedKeys(Relation rel, Set<FK> allFKs) {
		List<FK> fks = new ArrayList<FK>();
		for(FK fk: allFKs) {
			if( (rel.getSchemaName()==null || fk.pkTableSchemaName==null || rel.getSchemaName().equals(fk.pkTableSchemaName)) 
					&& rel.getName().equals(fk.pkTable)) {
				fks.add(fk);
			}
		}
		return fks;
	}
	
	public static List<Constraint> getUKs(Relation rel) {
		List<Constraint> uks = new ArrayList<Constraint>();
		List<Constraint> constraints = rel.getConstraints();
		if(constraints!=null) {
			for(Constraint c: constraints) {
				if(c.type==ConstraintType.UNIQUE) {
					uks.add(c);
				}
			}
		}
		return uks;
	}

}
