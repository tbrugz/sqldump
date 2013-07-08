package tbrugz.sqldump.def;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.Utils;

public abstract class AbstractDBMSFeatures implements DBMSFeatures {

	protected boolean grabExecutables = true;
	protected boolean grabIndexes = true;
	protected boolean grabSequences = true;
	protected boolean grabSynonyms = true;
	protected boolean grabTriggers = true;
	protected boolean grabViews = true;
	protected boolean grabUniqueConstraints = true;
	protected boolean grabCheckConstraints = true;

	@Override
	public void procProperties(Properties prop) {
		grabIndexes = Utils.getPropBool(prop, PROP_GRAB_INDEXES, grabIndexes);
		grabExecutables = Utils.getPropBool(prop, PROP_GRAB_EXECUTABLES, grabExecutables);
		grabSequences = Utils.getPropBool(prop, PROP_GRAB_SEQUENCES, grabSequences);
		grabSynonyms = Utils.getPropBool(prop, PROP_GRAB_SYNONYMS, grabSynonyms);
		grabTriggers = Utils.getPropBool(prop, PROP_GRAB_TRIGGERS, grabTriggers);
		grabViews = Utils.getPropBool(prop, PROP_GRAB_VIEWS, grabViews);
		grabUniqueConstraints = grabCheckConstraints = Utils.getPropBool(prop, PROP_GRAB_CONSTRAINTS_XTRA, grabUniqueConstraints);
	}
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata; //no decorator
	}

	@Override
	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}

	@Override
	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
	}

	@Override
	public void addFKSpecificFeatures(FK fk, ResultSet rs) {
	}
	
	@Override
	public Map<Class<?>, Class<?>> getColumnTypeMapper() {
		return null;
	}
	
	@Override
	public String sqlAlterColumnClause() {
		return "alter column";
	}
	
	@Override
	public String sqlAddColumnClause() {
		return "add column";
	}
	
	@Override
	public String sqlRenameColumnDefinition(NamedDBObject table, Column column, String newName) {
		//oracle & postgresql syntax
		return "alter table "+DBObject.getFinalName(table, true)+" rename column "+column.getName()+" TO "+newName;
	}
	
}
