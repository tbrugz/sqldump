package tbrugz.sqldump.processors;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.BodiedObject;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.RemarkableDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.def.AbstractSchemaProcessor;

//XXX: ContentNormalizerProcessor? BodyNormalizerProcessor?
public class ContentNormalizerProcessor extends AbstractSchemaProcessor {

	static final Log log = LogFactory.getLog(ContentNormalizerProcessor.class);

	public static final Pattern PTRN_TRAILING_CR = Pattern.compile("\r\n");
	public static final String NL = "\n";
	
	/*
	 * XXX: normalize VIEW's query?
	 * XXXxx: normalize TABLE's & COLUMN's remarks?
	 */
	@Override
	public void process() {
		long bodyCount = 0;
		log.debug("process()...");
		Set<ExecutableObject> execs = model.getExecutables();
		for(BodiedObject bo: execs) {
			if(bo.isDumpable()) {
				normalizeBody(bo);
				bodyCount++;
			}
		}
		Set<Trigger> triggers = model.getTriggers();
		for(BodiedObject bo: triggers) {
			if(bo.isDumpable()) {
				normalizeBody(bo);
				bodyCount++;
			}
		}
		log.info(bodyCount+" bodies normalized");
		
		long rCount = 0;
		Set<Table> tables = model.getTables();
		for(Table t: tables) {
			if(t.hasRemarks()) {
				normalizeRemarks(t);
				rCount++;
			}
			List<Column> cols = t.getColumns();
			for(Column c: cols) {
				if(c.hasRemarks()) {
					normalizeRemarks(c);
					rCount++;
				}
			}
		}
		log.info(rCount+" remarks normalized");
	}
	
	void normalizeBody(BodiedObject bo) {
		bo.setBody( normalize( bo.getBody() ) );
	}

	void normalizeRemarks(RemarkableDBObject ro) {
		ro.setRemarks( normalize( ro.getRemarks() ) );
	}
	
	public static String normalize(String s) {
		return PTRN_TRAILING_CR.matcher(s).replaceAll(NL);
	}

}
