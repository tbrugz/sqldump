package tbrugz.sqldump.xtraproc;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSchemaProcessor;
import tbrugz.sqldump.util.StringDecorator;

public class ModelSQLIdTransformer extends AbstractSchemaProcessor {

	static Log log = LogFactory.getLog(ModelSQLIdTransformer.class);

	StringDecorator identifierDecorator;
	StringDecorator colTypeDecorator;
	
	static final String SQLIDTRANSF_PREFFIX = "sqldump.proc.sqlidtransformer";
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		String defaultDec = prop.getProperty(SQLIDTRANSF_PREFFIX+".decorator");
		{
			String identifierDec = prop.getProperty(SQLIDTRANSF_PREFFIX+".iddecorator", defaultDec);
			identifierDecorator = StringDecorator.getDecorator(identifierDec);
		}
		{
			String colTypeDec = prop.getProperty(SQLIDTRANSF_PREFFIX+".coltypedecorator", defaultDec);
			colTypeDecorator = StringDecorator.getDecorator(colTypeDec);
		}
	}
	
	@Override
	public void process() {
		for(Table table: model.getTables()) {
			table.setName( identifierDecorator.get(table.getName()) );
			for(Column col: table.getColumns()) {
				col.setName( identifierDecorator.get(col.getName()) );
				col.setType( colTypeDecorator.get(col.getType()) );
			}
			for(Constraint c: table.getConstraints()) {
				c.setName( identifierDecorator.get(c.getName()) );
				procList(c.getUniqueColumns(), identifierDecorator);
			}
		}
		for(FK fk: model.getForeignKeys()) {
			fk.setName( identifierDecorator.get(fk.getName()) );
			fk.setPkTable( identifierDecorator.get( fk.getPkTable() ) );
			fk.setFkTable( identifierDecorator.get( fk.getFkTable() ) );
			procList(fk.getFkColumns(), identifierDecorator);
			procList(fk.getPkColumns(), identifierDecorator);
		}
		for(Index i: model.getIndexes()) {
			i.setName( identifierDecorator.get(i.getName()) );
			i.setTableName(identifierDecorator.get( i.getTableName() ));
			procList(i.getColumns(), identifierDecorator);
		}
		log.info("model transformer end: ok");
	}
	
	void procList(List<String> list, StringDecorator decorator) {
		for(int i=0; i<list.size(); i++) {
			list.set(i, decorator.get(list.get(i)) );
		}
	}

}
