package tbrugz.sqldump.xtraproc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.util.MapEntryValueComparator;

public class StatsProc extends AbstractSQLProc {

	static Log log = LogFactory.getLog(StatsProc.class);

	//Map<String, Integer> mapCounts = new HashMap<String, Integer>();
	
	/*public static class ValueComparator implements Comparator<Object> {

		Map<?,?> base;
		public ValueComparator(Map<?,?> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			Object va = base.get(a);
			Object vb = base.get(b);
			log.warn("comp: "+a+":"+b+" / "+va+":"+vb);
			//XXX: not good if compare returns 0 (no partial order)
			if(va instanceof Comparable) {
				return ((Comparable) va).compareTo(vb);
			}
			log.warn("err: "+a+":"+b+" / "+va+":"+vb);
			
			return 0;
		}
	}*/
	
	
	/*public static Set sortMapKeysByValue(Map map, Comparator comp) {
	}*/
	
	@Override
	public void process() {
		Map<String,Integer> map = new HashMap<String,Integer>();

		try {
			for(Table table: model.getTables()) {
				ResultSet rs = conn.createStatement().executeQuery("select count(*) from "+table.getSchemaName()+"."+table.getName());
				rs.next();
				int count = rs.getInt(1);
				map.put(table.getName(), count);
				//log.info("count["+table.getName()+"]: "+count);
			}
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		
		/*for (String key: map.keySet()) {
			System.out.println("map key/value: " + key + "/"+map.get(key));
		}*/

		//ValueComparator bvc =  new ValueComparator(map);
		//Map<String,Integer> sorted_map = new TreeMap<String,Integer>(bvc);
		//sorted_map.putAll(map);
		Map<String,Integer> sorted_map = MapEntryValueComparator.sortByValue(map, true);
		
		for (String key: sorted_map.keySet()) {
			System.out.println("count["+key+"]: "+sorted_map.get(key));
		}
	}

}
