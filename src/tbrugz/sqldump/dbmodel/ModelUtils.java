package tbrugz.sqldump.dbmodel;

import java.util.Collection;

public class ModelUtils {

	public static Collection<? extends DBIdentifiable> getCollectionFromModel(SchemaModel model, DBObjectType type) {
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

}
