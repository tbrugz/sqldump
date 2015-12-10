package tbrugz.sqldump.xtraproc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.MapEntryValueComparator;

/*
 * should this be a "data profiler"? 
 * see: http://wiki.pentaho.com/display/EAI/Kettle+Data+Profiling+with+DataCleaner , https://en.wikipedia.org/wiki/Data_profiling
 * 
 * XXX: CategorizedOut: use [schemaname]?
 */
public class StatsProc extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(StatsProc.class);

	static final String PROP_PREFIX = "sqldump.statsproc";

	static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	static final String PROP_MAP_ORDER = PROP_PREFIX+".x-order";
	
	String fileOutput;
	boolean orderByValue = false;
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		fileOutput = prop.getProperty(PROP_OUTFILEPATTERN, CategorizedOut.STDOUT);
		orderByValue = "value".equalsIgnoreCase(prop.getProperty(PROP_MAP_ORDER));
	}
	
	@Override
	public void process() {
		try {
			processIntern();
		}
		catch(IOException e) {
			log.warn("StatsProc error: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}
	
	void processIntern() throws IOException {
		Map<String,Integer> map = new HashMap<String,Integer>();

		//String finalPattern = CategorizedOut.generateFinalOutPattern(fileOutput, (String[]) null);
		CategorizedOut cout = new CategorizedOut(fileOutput);
		
		try {
			for(Table table: model.getTables()) {
				ResultSet rs = conn.createStatement().executeQuery("select count(*) from "+table.getFinalQualifiedName());
				rs.next();
				int count = rs.getInt(1);
				map.put(table.getName(), count);
				//log.info("count["+table.getName()+"]: "+count);
			}
		}
		catch(SQLException e) {
			log.warn("error counting rows", e);
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
		log.info("stats for "+count+" tables dumped, outpattern: "+fileOutput);
	}

}
