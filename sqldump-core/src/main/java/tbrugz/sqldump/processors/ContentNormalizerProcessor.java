package tbrugz.sqldump.processors;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.BodiedObject;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.RemarkableDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.def.AbstractSchemaProcessor;

public class ContentNormalizerProcessor extends AbstractSchemaProcessor {

	static final Log log = LogFactory.getLog(ContentNormalizerProcessor.class);

	public static final Pattern PTRN_TRAILING_CR = Pattern.compile("\r\n");
	public static final String NL = "\n";
	
	boolean normalizeDeclaration = true;
	
	/*
	 * XXX: normalize VIEW's query?
	 * XXXxx: normalize TABLE's & COLUMN's remarks?
	 */
	@Override
	public void process() {
		long bodyCount = 0;
		long declarationCount = 0;
		
		log.debug(getIdDesc()+"process()...");
		Set<ExecutableObject> execs = model.getExecutables();
		for(ExecutableObject bo: execs) {
			if(isNormalizeable(bo)) {
				normalizeBody(bo);
				bodyCount++;
				if(normalizeDeclaration) {
					normalizeDeclaration(bo);
					declarationCount++;
				}
			}
		}
		Set<Trigger> triggers = model.getTriggers();
		for(Trigger bo: triggers) {
			if(bo.isDumpable()) {
				normalizeBody(bo);
				bodyCount++;
			}
			if(normalizeDeclaration) {
				normalizeDescription(bo);
				declarationCount++;
			}
		}
		log.info(getIdDesc()+bodyCount+" bodies normalized");
		if(normalizeDeclaration) {
			log.info(getIdDesc()+declarationCount+" declarations/descriptions normalized");
		}

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
		log.info(getIdDesc()+rCount+" remarks normalized");
	}
	
	boolean isNormalizeable(ExecutableObject eo) {
		if(!eo.isDumpable()) { return false; }
		DBObjectType dbtype = eo.getDbObjectType();
		return DBObjectType.JAVA_SOURCE!=dbtype;
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

	void normalizeDeclaration(ExecutableObject eo) {
		/*DBObjectType dbtype = eo.getDbObjectType();
		if(dbtype!=null && dbtype==DBObjectType.JAVA_SOURCE) {
			log.warn("[normalizeDeclaration] object "+eo.getFinalQualifiedName()+" [type "+dbtype+"] won't have declaration normalized");
			return;
		}*/
		eo.setBody( normalizeDeclaration( eo.getBody() ) );
	}

	void normalizeDescription(Trigger t) {
		t.setDescription( normalizeTriggerDescription( t.getDescription() ) );
	}
	
	public static final Pattern PTRN_CREATE_EX  = Pattern.compile("\\s*(create(\\s+or\\s+replace|)\\s+|)(procedure|function|package(\\s+body?|)|trigger|type)\\s+(([\\p{Alnum}_]+)\\.|)([\\p{Alnum}_]+)", Pattern.CASE_INSENSITIVE);
	public static final Pattern PTRN_CREATE_EXQ = Pattern.compile("\\s*(create(\\s+or\\s+replace|)\\s+|)(procedure|function|package(\\s+body?|)|trigger|type)\\s+(\"([\\p{Alnum}_]+)\"\\.|)\"([\\p{Alnum}_]+)\"", Pattern.CASE_INSENSITIVE);

	public static String normalizeDeclaration(String s) {
		Matcher m = PTRN_CREATE_EXQ.matcher(s);
		boolean found = false;
		if(m.find()) {
			//log.info("match1 (quote)");
			found = true;
		}
		else {
			m = PTRN_CREATE_EX.matcher(s);
			if(m.find()) {
				//log.info("match2 (non-quote)");
				found = true;
			}
		}
		if(found) {
			//log.info("match!");
			StringBuilder sb = new StringBuilder();
			String create = m.group(1);
			if(!create.equals("")) {
				sb.append("create ");
				String orReplace = m.group(2);
				if(orReplace!=null && !orReplace.equals("")) {
					sb.append("or replace ");
					//log.info("orReplace=["+orReplace+"]");
				}
			}
			String type = m.group(3).toLowerCase();
			type = type.replaceAll("\\s+", " ");
			sb.append(type);
			//log.info("type=["+type.toLowerCase()+"]");
			sb.append(" ");
			String schemaName = m.group(6);
			if(schemaName==null || schemaName.equals("")) {}
			else {
				sb.append(schemaName.toUpperCase());
				sb.append(".");
				//log.info("schemaName=["+schemaName.toUpperCase()+"]");
			}
			String objName = m.group(7);
			sb.append(objName.toUpperCase());
			//log.info("objName=["+objName.toUpperCase()+"]");
			sb.append(s.substring(m.group().length()));
			return sb.toString();
		}
		else {
			log.warn("[normalizeDeclaration] not match: "+substr(s, 80));
		}
		return s;
	}

	public static final Pattern PTRN_DESCRIPTION  = Pattern.compile("\\s*(([\\p{Alnum}_]+)\\.|)([\\p{Alnum}_]+)", Pattern.CASE_INSENSITIVE);
	public static final Pattern PTRN_DESCRIPTIONQ = Pattern.compile("\\s*(\"([\\p{Alnum}_]+)\"\\.|)\"([\\p{Alnum}_]+)\"", Pattern.CASE_INSENSITIVE);

	public static String normalizeTriggerDescription(String s) {
		Matcher m = PTRN_DESCRIPTIONQ.matcher(s);
		boolean found = false;
		if(m.find()) {
			//log.info("match1 (quote)");
			found = true;
		}
		else {
			//log.info("not match1 (quote): "+substr(s, 50));
			m = PTRN_DESCRIPTION.matcher(s);
			if(m.find()) {
				//log.info("match2 (non-quote)");
				found = true;
			}
		}
		if(found) {
			//log.info("match!");
			StringBuilder sb = new StringBuilder();
			String schemaName = m.group(2);
			if(schemaName==null || schemaName.equals("")) {}
			else {
				sb.append(schemaName.toUpperCase());
				sb.append(".");
				//log.info("schemaName=["+schemaName.toUpperCase()+"]");
			}
			String objName = m.group(3);
			sb.append(objName.toUpperCase());
			//log.info("objName=["+objName.toUpperCase()+"]");
			sb.append(s.substring(m.group().length()));
			return sb.toString();
		}
		else {
			log.warn("[normalizeTriggerDescription] not match: "+substr(s, 80));
		}
		return s;
	}
	
	static String substr(String s, int x) {
		if(s.length()>x) { return s.substring(0, x-3)+"..."; }
		return s;
	}

}
