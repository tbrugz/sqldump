package tbrugz.sqldump;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public abstract class AbstractDBMSFeatures implements DBMSFeatures {

	protected boolean grabExecutables = true;
	protected boolean grabIndexes = true;
	protected boolean grabSequences = true;
	protected boolean grabSynonyms = true;
	protected boolean grabTriggers = true;
	protected boolean grabViews = true;
	protected boolean grabExtraConstraints = true;

	public void procProperties(Properties prop) {
		grabIndexes = Utils.getPropBool(prop, PROP_GRAB_INDEXES, grabIndexes);
		grabExecutables = Utils.getPropBool(prop, PROP_GRAB_EXECUTABLES, grabExecutables);
		grabSequences = Utils.getPropBool(prop, PROP_GRAB_SEQUENCES, grabSequences);
		grabSynonyms = Utils.getPropBool(prop, PROP_GRAB_SYNONYMS, grabSynonyms);
		grabTriggers = Utils.getPropBool(prop, PROP_GRAB_TRIGGERS, grabTriggers);
		grabViews = Utils.getPropBool(prop, PROP_GRAB_VIEWS, grabViews);
		grabExtraConstraints = Utils.getPropBool(prop, PROP_GRAB_CONSTRAINTS_XTRA, grabExtraConstraints);
	}
	
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata; //no decorator
	}

	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}

	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
	}
}
