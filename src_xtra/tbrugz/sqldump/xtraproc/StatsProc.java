package tbrugz.sqldump.xtraproc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.MapEntryValueComparator;

/*
 * should this be a "data profiler"? 
 * see: http://wiki.pentaho.com/display/EAI/Kettle+Data+Profiling+with+DataCleaner
 * 
 * TODO: use tbrugz.sqldump.util.CategorizedOut ?
 */
public class StatsProc extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(StatsProc.class);
	
	@Override
	public void process() {
		Map<String,Integer> map = new HashMap<String,Integer>();

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
		
		/*for (String key: map.keySet()) {
			System.out.println("map key/value: " + key + "/"+map.get(key));
		}*/

		Map<String,Integer> sorted_map = MapEntryValueComparator.sortByValue(map, true);
		
		for(Entry<String,Integer> entry: sorted_map.entrySet()) {
			System.out.println("count["+entry.getKey()+"]: "+entry.getValue());
		}
	}

}
