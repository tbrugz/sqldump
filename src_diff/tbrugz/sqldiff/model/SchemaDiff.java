package tbrugz.sqldiff.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.compare.ExecOrderDiffComparator;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.util.CategorizedOut;

//XXX: should SchemaDiff implement Diff?
//XXX: what about renames?
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SchemaDiff implements Diff {
	static final Log log = LogFactory.getLog(SchemaDiff.class);

	//XXX: should be List<>?
	@XmlElement(name="tableDiff")
	final Set<TableDiff> tableDiffs = new TreeSet<TableDiff>();
	@XmlElement(name="columnDiff")
	final Set<ColumnDiff> columnDiffs = new TreeSet<ColumnDiff>();
	@XmlElement(name="grantDiff")
	final Set<GrantDiff> grantDiffs = new TreeSet<GrantDiff>();
	@XmlElement(name="dbidDiff")
	final Set<DBIdentifiableDiff> dbidDiffs = new TreeSet<DBIdentifiableDiff>();
	
	//XXX add String sqlDialect; ?

	@SuppressWarnings("unchecked")
	public static void logInfo(SchemaDiff diff) {
		int maxNameSize = getMaxDBObjectNameSize(diff.tableDiffs, diff.columnDiffs, diff.dbidDiffs);
		logInfoByObjectAndChangeType(diff.tableDiffs, maxNameSize);
		logInfoByObjectAndChangeType(diff.columnDiffs, maxNameSize);
		logInfoByObjectAndChangeType(diff.grantDiffs, maxNameSize);
		logInfoByObjectAndChangeType(diff.dbidDiffs, maxNameSize);
	}

	static void logInfoByObjectAndChangeType(Collection<? extends Diff> diffs, int labelSize) {
		//Map<DBObjectType, Integer> map = new NeverNullGetMap<DBObjectType, Integer>(Integer.class);
		//String format = "changes [%-12s]: ";
		String formatStr = "changes [%-"+labelSize+"s]:";
		if(labelSize<=0) {
			formatStr = "changes [%s]:";
		}
		for(DBObjectType type: DBObjectType.values()) {
			List<Diff> diffsoftype = getDiffsByDBObjectType(diffs, type);
			StringBuffer sb = new StringBuffer();
			boolean changed = false;
			sb.append(String.format(formatStr, type));
			//sb.append("changes ["+type+"]: ");
			for(ChangeType ct: ChangeType.values()) {
				int size = getDiffOfChangeType(ct, diffsoftype).size();
				if(size>0) {
					sb.append(" "+ct+"("+getDiffOfChangeType(ct, diffsoftype).size()+")");
					changed = true;
				}
			}
			if(changed) {
				log.info(sb.toString());
			}
		}
		
		if(log.isDebugEnabled()) {
			for(Diff d: diffs) {
				log.debug("diff: obj = "+d.getNamedObject().getSchemaName()+"."+d.getNamedObject().getName()+" ; type = "+d.getObjectType());
			}
		}
	}

	public static Collection<? extends Diff> getDiffOfChangeType(ChangeType changeType, Collection<? extends Diff> list) {
		Collection<Diff> ret = new ArrayList<Diff>();
		for(Diff d: list) {
			if(changeType.equals(d.getChangeType())) { ret.add(d); }
		}
		return ret;
	}
	
	static int getMaxDBObjectNameSize(Collection<? extends Diff>... diffs) {
		int max = 0;
		for(Collection<? extends Diff> c: diffs) {
			for(Diff d: c) {
				int size = d.getObjectType().name().length();
				if(size>max) { max = size; }
			}
		}
		return max;
	}
	
	static List<Diff> getDiffsByDBObjectType(Collection<? extends Diff> diffs, DBObjectType dbtype) {
		List<Diff> retdiff = new ArrayList<Diff>();
		for(Diff d: diffs) {
			if(dbtype.equals(d.getObjectType())) {
				retdiff.add(d);
			}
		}
		return retdiff;
	}
	
	//@Override
	public List<Diff> getChildren() {
		List<Diff> diffs = new ArrayList<Diff>();
		diffs.addAll(tableDiffs);
		diffs.addAll(columnDiffs);
		diffs.addAll(grantDiffs);
		diffs.addAll(dbidDiffs);
		
		//XXX: option to select diff comparator?
		Collections.sort(diffs, new ExecOrderDiffComparator());
		return diffs;
	}
	
	public void outDiffs(CategorizedOut out) throws IOException {
		List<Diff> diffs = getChildren();

		log.info("output diffs...");
		int count = 0;
		for(Diff d: diffs) {
			String schemaName = d.getNamedObject()!=null?d.getNamedObject().getSchemaName():"";
			log.debug("diff: "+d.getChangeType()+" ; "+DBIdentifiable.getType4Alter(d.getObjectType()).name()
					+" ; "+schemaName+"; "+d.getNamedObject().getName());
			//XXXdone: if diff is ADD+EXECUTABLE, not to include ';'? yep
			String append = (d.getObjectType().isExecutableType()
					&& d.getChangeType().equals(ChangeType.ADD)) 
					? "\n" : ";\n";
			
			out.categorizedOut(d.getDiff()+append,
					schemaName,
					DBIdentifiable.getType4Alter(d.getObjectType()).name(),
					d.getNamedObject().getName(),
					d.getChangeType().name()
					);
			count++;
		}
		log.info(count+" diffs dumped");
	}
	
	@Override
	public String getDiff() {
		StringBuffer sb = new StringBuffer();
		
		List<Diff> diffs = getChildren();

		for(Diff d: diffs) {
			//XXX: if diff is ADD+EXECUTABLE, not to include ';'?
			//XXX: use getDiffList()?
			sb.append(d.getDiff()+";\n\n");
		}

		/*
		//tables
		for(TableDiff td: tableDiffs) {
			sb.append(td.getDiff()+";\n\n");
		}
		//columns
		for(TableColumnDiff tcd: columnDiffs) {
			sb.append(tcd.getDiff()+";\n\n");
		}
		//dbidentifiable
		for(DBIdentifiableDiff dbdiff: dbidDiffs) {
			sb.append(dbdiff.getDiff()+";\n\n");
		}
		*/
		
		return sb.toString();
	}
	
	@Override
	public List<String> getDiffList() {
		List<Diff> diffs = getChildren();
		List<String> diffStrs = new ArrayList<String>();

		for(Diff d: diffs) {
			List<String> ls = d.getDiffList();
			if(ls==null) {
				//log.warn("null diff...");
				continue;
			}
			for(String s: ls) {
				diffStrs.add(s);
			}
		}
		
		return diffStrs;
	}
	
	@Override
	public String toString() {
		return "[SchemaDiff: tables: #"+tableDiffs.size()+", cols: #"+columnDiffs.size()+", xtra: #"+dbidDiffs.size()+"]"; //XXX: A(dd), M(modified), R(emoved)
	}

	@Override
	public ChangeType getChangeType() {
		return null; //XXX: SchemaDiff.ChangeType?
	}

	@Override
	public DBObjectType getObjectType() {
		return null; //XXX: SchemaDiff.DBObjectType?
	}
	
	@Override
	public NamedDBObject getNamedObject() {
		return null; //XXX: SchemaDiff.getNamedObject
	}
	
	@Override
	public SchemaDiff inverse() {
		//List<Diff<?>> dlist = getChildren();
		SchemaDiff inv = new SchemaDiff();
		//List<Diff<?>> invlist = new ArrayList<Diff<?>>();
		for(TableDiff d: tableDiffs) {
			inv.tableDiffs.add(d.inverse());
		}
		for(ColumnDiff d: columnDiffs) {
			inv.columnDiffs.add(d.inverse());
		}
		for(DBIdentifiableDiff d: dbidDiffs) {
			inv.dbidDiffs.add(d.inverse());
		}
		
		return inv;
	}
	
	public Set<TableDiff> getTableDiffs() {
		return tableDiffs;
	}

	public Set<ColumnDiff> getColumnDiffs() {
		return columnDiffs;
	}

	public Set<DBIdentifiableDiff> getDbIdDiffs() {
		return dbidDiffs;
	}
	
	public Set<GrantDiff> getGrantDiffs() {
		return grantDiffs;
	}

	@Override
	public String getDefinition() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String getPreviousDefinition() {
		// TODO Auto-generated method stub
		return null;
	}
}
