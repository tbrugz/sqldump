package tbrugz.sqldump;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public abstract class AbstractDBMSFeatures implements DBMSFeatures {

	protected boolean grabExecutables = false;
	protected boolean grabIndexes = false;
	protected boolean grabSequences = false;
	protected boolean grabSynonyms = false;
	protected boolean grabTriggers = false;
	protected boolean grabViews = false;

	public void procProperties(Properties prop) {
		grabIndexes = Utils.getPropBool(prop, PROP_GRAB_INDEXES, true);
		grabExecutables = Utils.getPropBool(prop, PROP_GRAB_EXECUTABLES, true);
		grabSequences = Utils.getPropBool(prop, PROP_GRAB_SEQUENCES, true);
		grabSynonyms = Utils.getPropBool(prop, PROP_GRAB_SYNONYMS, true);
		grabTriggers = Utils.getPropBool(prop, PROP_GRAB_TRIGGERS, true);
		grabViews = Utils.getPropBool(prop, PROP_GRAB_VIEWS, true);
	}
	
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata; //no decorator
	}

	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}

	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
	}
}
