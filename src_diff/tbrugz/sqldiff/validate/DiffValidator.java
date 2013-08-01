package tbrugz.sqldiff.validate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.DBIdentifiableDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

public class DiffValidator {
	
	static final Log log = LogFactory.getLog(DiffValidator.class);
	final SchemaModel model;
	
	public DiffValidator(SchemaModel model) {
		this.model = model;
	}
	
	//XXX: validate diffs idependently or do 'composite' validation (applying changes into temp model)?
	public void validateDiff(Diff diff) {
		if(diff instanceof SchemaDiff) {
			for(Diff d: ((SchemaDiff) diff).getChildren()) {
				validateDiff(d);
			}
			log.info("schemadiff '"+diff+"' validated");
		}
		else if(diff instanceof TableDiff) {
			validateTableDiff((TableDiff) diff);
		}
		else if(diff instanceof ColumnDiff) {
			validateColumnDiff((ColumnDiff) diff);
		}
		else if(diff instanceof DBIdentifiableDiff) {
			//other dbobjects?
			log.debug("no validation applied to dbIdDiff '"+diff+"'");
		}
		else {
			//error
			throw new IllegalStateException("can't validate diff of unknown type ["+diff+"]");
		}
		//log.debug("diff '"+diff+"' validated");
	}
	
	void validateTableDiff(TableDiff td) {
		//add table: model must not contain table 
		//drop table: model must contain table
		Table tNew = DBIdentifiable.getDBIdentifiableByNamedObject(model.getTables(), td.getNamedObject());
		switch(td.getChangeType()) {
		case ADD: 
			if(tNew!=null) throw new IncompatibleChangeException("can't ADD a table that already exists ["+tNew+"]");
			break;
		case DROP: 
			if(tNew==null) throw new IncompatibleChangeException("can't DROP a table that does not exists ["+td.getNamedObject()+"]");
			break;
		case RENAME:
			if(tNew!=null) throw new IncompatibleChangeException("can't RENAME a table to a name that already exists ["+tNew+"]");
			Table tPrev = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTables(), td.getRenameFromSchema(), td.getRenameFromName());
			if(tPrev==null) throw new IncompatibleChangeException("can't RENAME a table that does not exists ["+td.getTable()+"]");
			break;
		case ALTER:
			throw new IllegalStateException(td.getChangeType()+" change for TableDiff not supported");
		}
	}
	
	void validateColumnDiff(ColumnDiff cd) {
		//add column: model must not contain column 
		//drop column: model must contain column
		//rename column: model must contain original column & not contain new column
		//alter column: model must contain column
		Table table = DBIdentifiable.getDBIdentifiableByNamedObject(model.getTables(), cd.getNamedObject());
		if(table==null) {
			throw new IncompatibleChangeException("can't change a column for non-existent table ["+cd.getNamedObject()+"]");
		}
		switch (cd.getChangeType()) {
		case ADD: {
			Column c = table.getColumn(cd.getColumn().getName());
			if(c!=null) throw new IncompatibleChangeException("can't ADD a column that already exists ["+cd.getColumn()+"]");
			break; }
		case DROP: {
			Column c = table.getColumn(cd.getPreviousColumn().getName());
			if(c==null) throw new IncompatibleChangeException("can't DROP a column that does not exists ["+cd.getPreviousColumn()+"]");
			break; }
		case ALTER: {
			Column c = table.getColumn(cd.getPreviousColumn().getName());
			if(c==null) throw new IncompatibleChangeException("can't ALTER a column that does not exists ["+cd.getPreviousColumn()+"]");
			break; }
		case RENAME: {
			Column cPrev = table.getColumn(cd.getPreviousColumn().getName());
			if(cPrev==null) throw new IncompatibleChangeException("can't RENAME a column that does not exists ["+cd.getPreviousColumn()+"]");
			Column cNew = table.getColumn(cd.getColumn().getName());
			if(cNew!=null) throw new IncompatibleChangeException("can't RENAME a column to a name that already exists ["+cd.getColumn()+"]");
			break; }
		}
	}

}
