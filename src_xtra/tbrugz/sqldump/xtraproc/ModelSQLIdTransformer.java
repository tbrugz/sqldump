package tbrugz.sqldump.xtraproc;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.util.StringDecorator;

public class ModelSQLIdTransformer extends AbstractSQLProc {

	static Log log = LogFactory.getLog(ModelSQLIdTransformer.class);

	StringDecorator identifierDecorator = new StringDecorator.StringToLowerDecorator();
	StringDecorator colTypeDecorator = new StringDecorator.StringToLowerDecorator();
	
	@Override
	public void process() {
		for(Table table: model.getTables()) {
			table.setName( identifierDecorator.get(table.getName()) );
			for(Column col: table.getColumns()) {
				col.setName( identifierDecorator.get(col.getName()) );
				col.type = colTypeDecorator.get(col.type);
			}
			for(Constraint c: table.getConstraints()) {
				c.setName( identifierDecorator.get(c.getName()) );
				procList(c.uniqueColumns, identifierDecorator);
			}
		}
		for(FK fk: model.getForeignKeys()) {
			fk.setName( identifierDecorator.get(fk.getName()) );
			fk.pkTable = identifierDecorator.get( fk.pkTable );
			fk.fkTable = identifierDecorator.get( fk.fkTable );
			procList(fk.fkColumns, identifierDecorator);
			procList(fk.pkColumns, identifierDecorator);
		}
		for(Index i: model.getIndexes()) {
			i.setName( identifierDecorator.get(i.getName()) );
			i.tableName = identifierDecorator.get( i.tableName );
			procList(i.columns, identifierDecorator);
		}
		log.info("model transformer end: ok");
	}
	
	void procList(List<String> list, StringDecorator decorator) {
		for(int i=0; i<list.size(); i++) {
			list.set(i, decorator.get(list.get(i)) );
		}
	}

}
