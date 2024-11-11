package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.util.LongFactory;
import tbrugz.util.NonNullGetMap;

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
			if( (rel.getSchemaName()==null || fk.getFkTableSchemaName()==null || rel.getSchemaName().equals(fk.getFkTableSchemaName())) 
					&& rel.getName().equals(fk.fkTable)) {
				fks.add(fk);
			}
		}
		return fks;
	}

	public static List<FK> getExportedKeys(Relation rel, Set<FK> allFKs) {
		List<FK> fks = new ArrayList<FK>();
		for(FK fk: allFKs) {
			if( (rel.getSchemaName()==null || fk.getPkTableSchemaName()==null || rel.getSchemaName().equals(fk.getPkTableSchemaName())) 
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
	
	public static Constraint getPkOrUk(Relation relation) {
		Constraint pk = null;
		List<Constraint> conss = relation.getConstraints();
		if(conss!=null) {
			Constraint uk = null;
			for(Constraint c: conss) {
				if(c.getType()==ConstraintType.PK) { pk = c; break; }
				if(c.getType()==ConstraintType.UNIQUE && uk == null) { uk = c; }
			}
			if(pk == null && uk != null) {
				pk = uk;
			}
		}
		return pk;
	}

	public static String getExecutableCountsByType(Set<ExecutableObject> execs) {
		StringBuilder sb = new StringBuilder();
		Map<DBObjectType, Long> map = new NonNullGetMap<DBObjectType, Long>(new TreeMap<DBObjectType, Long>(), new LongFactory());
		for(ExecutableObject eo: execs) {
			map.put(eo.getType(), map.get(eo.getType())+1);
		}
		boolean is1st = true;
		for(Map.Entry<DBObjectType, Long> e: map.entrySet()) {
			if(is1st) { is1st = false; } else { sb.append("\n"); }
			sb.append(e.getKey()+": "+e.getValue());
		}
		return sb.toString();
	}

}
