package tbrugz.sqldump.xtraproc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.MapEntryValueComparator;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * should this be a "data profiler"? 
 * see: http://wiki.pentaho.com/display/EAI/Kettle+Data+Profiling+with+DataCleaner , https://en.wikipedia.org/wiki/Data_profiling
 * 
 * http://www.programmerinterview.com/index.php/database-sql/cardinality-versus-selectivity/
 * select count(*), count(distinct COL) from TABLE
 * 
 * http://www.slideshare.net/khuranashailja/data-quality-and-data-profiling
 *
 * XXX: CategorizedOut: use [schemaname]?
 */
public class StatsProc extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(StatsProc.class);

	static final String PROP_PREFIX = "sqldump.statsproc";

	//static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	static final String PROP_COUNTS_BY_TABLE = PROP_PREFIX+".counts-by-table";
	static final String PROP_STATS_BY_COLUMN = PROP_PREFIX+".stats-by-column";
	
	static final String PROP_CBT_FILEOUT = PROP_COUNTS_BY_TABLE+".outfilepattern";
	static final String PROP_CBT_MAP_ORDER = PROP_COUNTS_BY_TABLE+".x-order";

	static final String PROP_SBC_FILEOUT = PROP_STATS_BY_COLUMN+".outfilepattern";
	
	//String fileOutput;
	boolean countsByTable = false;
	boolean statsByColumn = false;

	String cbtFileOutput;
	boolean orderByValue = false;

	String sbcFileOutput;
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		countsByTable = Utils.getPropBool(prop, PROP_COUNTS_BY_TABLE, countsByTable);
		statsByColumn = Utils.getPropBool(prop, PROP_STATS_BY_COLUMN, statsByColumn);
		
		cbtFileOutput = prop.getProperty(PROP_CBT_FILEOUT, CategorizedOut.STDOUT);
		orderByValue = "value".equalsIgnoreCase(prop.getProperty(PROP_CBT_MAP_ORDER));
		
		sbcFileOutput = prop.getProperty(PROP_SBC_FILEOUT, CategorizedOut.STDOUT);
	}
	
	@Override
	public void process() {
		try {
			if(countsByTable) { doCountsByTable(); }
			if(statsByColumn) { doStatsByColumn(); }
			if(!countsByTable && !statsByColumn) {
				log.warn("no 'countsByTable' (prop 'counts-by-table') nor 'statsByColumn' (prop 'stats-by-column') activated...");
			}
		}
		catch(IOException e) {
			log.warn("StatsProc error: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}
	
	void doCountsByTable() throws IOException {
		Map<String,Integer> map = new HashMap<String,Integer>();

		CategorizedOut cout = new CategorizedOut(cbtFileOutput);
		String sql = null;
		
		try {
			for(Table table: model.getTables()) {
				sql = "select count(*) from "+table.getFinalQualifiedName();
				ResultSet rs = conn.createStatement().executeQuery(sql);
				rs.next();
				int count = rs.getInt(1);
				map.put(table.getName(), count);
				//log.info("count["+table.getName()+"]: "+count);
			}
		}
		catch(SQLException e) {
			log.warn("doCountsByTable: error counting rows [sql="+sql+"]", e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
		
		Map<String,Integer> sorted_map = orderByValue?
				MapEntryValueComparator.sortByValue(map, true) :
				new TreeMap<String, Integer>(map);
		
		int count = 0;
		for(Entry<String,Integer> entry: sorted_map.entrySet()) {
			//System.out.println("count["+entry.getKey()+"]: "+entry.getValue());
			cout.categorizedOut(entry.getKey()+": count = "+entry.getValue());
			count++;
		}
		cout.getCategorizedWriter().close();
		log.info("stats [CountsByTable] for "+count+" tables dumped, outpattern: "+cbtFileOutput);
	}

	static final String[] NDCT = {"BLOB", "CLOB", "LONG" };
	
	boolean isNonDistinctableColumnType(String ct) {
		for(int i=0;i<NDCT.length;i++) {
			if(NDCT[i].equalsIgnoreCase(ct)) { return true; }
		}
		return false;
	}
	/*
	 * TODO: change ResultSet output processor to SQLQueries/DataDump
	 */
	void doStatsByColumn() throws IOException {
		CategorizedOut cout = new CategorizedOut(sbcFileOutput);
		
		int countTable = 0, countColumn = 0, countLobColumn = 0;
		try {
			List<String> cols = new ArrayList<String>();
			StringDecorator sd = new StringDecorator.StringQuoterDecorator("\"");
			
			for(Table table: model.getTables()) {
				for(Column c: table.getColumns()) {
					String n = sd.get(c.getName());
					String t = table.getName();
					String a = c.getName();
					String sqlPrefix = "select '"+t+"' as table_name, '"+a+"' as column_name, "+
							"'"+c.getType()+"' as type_name, "+
							c.getColumSize()+" as column_size, "+
							//c.getDecimalDigits()+" as decimal_digits, '"+
							//c.isNullable()+"' as nullable,"+
							"count(*) as count_all, ";
					boolean isLob = isNonDistinctableColumnType(c.getType());
					//XXX: add type, precision, scale?
					if(isLob) {
						cols.add(sqlPrefix
								+ "null as count_non_null, "
								+ "null as count_null, "
								+ "null as cardinality, null as selectivity "
								+ "from "+table.getFinalQualifiedName());
						countLobColumn++;
					}
					else {
						cols.add(sqlPrefix
								+ "count("+n+") as count_non_null, "
								+ "count(*) - count("+n+") as count_null, "
								+ "count(distinct "+n+") as cardinality, case when count("+n+")=0 then null else cast(count(distinct "+n+") as float)/count("+n+") end as selectivity "
								+ "from "+table.getFinalQualifiedName());
					}
					//cols.add("count("+n+") as "+a+"_count, count(distinct "+n+") as "+a+"_cardinality, cast(count(distinct "+n+") as float)/count("+n+") as "+a+"_selectivity");
					countColumn++;
				}
				countTable++;
				/*
				//String sql = "select "+Utils.join(cols, ", ")+" from "+table.getFinalQualifiedName();
				String sql = Utils.join(cols, "\nunion all\n");
				log.info("sql: "+sql);
				ResultSet rs2 = conn.createStatement().executeQuery(sql);
				System.out.println("<<-- "+table.getFinalQualifiedName()+" -->>");
				SQLUtils.dumpRS(rs2);
				System.out.println("<<-- --------------------------------- -->>"); */
			}
			
			//String sql = "select "+Utils.join(cols, ", ")+" from "+table.getFinalQualifiedName();
			String sql = Utils.join(cols, "\nunion all\n")+"\norder by table_name, column_name";
			//log.info("sql: "+sql);
			ResultSet rs2 = conn.createStatement().executeQuery(sql);
			//System.out.println("<<-- --------------------------------- -->>");
			//log.info("cout: "+cout.getCategorizedWriter());
			SQLUtils.dumpRS(rs2, cout.getCategorizedWriter());
			cout.getCategorizedWriter().close();
			//System.out.println("<<-- --------------------------------- -->>");
			
		}
		catch(SQLException e) {
			log.warn("doStatsByColumn: error counting rows", e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
		
		log.info("stats [StatsByColumn] for "+countTable+" tables & "+countColumn+" columns ["+countLobColumn+" LOBs] dumped, outpattern: "+sbcFileOutput);
	}
	
}
