package tbrugz.sqldump.resultset;

import java.beans.IntrospectionException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.View;

public class RsListAdapterModelTest {

	List<String> statusUniqueColumns = Arrays.asList(new String[]{"schemaName", "name"});
	
	@Test
	public void testView() throws IntrospectionException, SQLException {
		List<View> list = new ArrayList<View>();
		View v = new View();
		v.setName("a");
		list.add(v);
		
		ResultSet rs = new ResultSetListAdapter<View>("views", statusUniqueColumns, list, View.class);
		ResultSetMetaData md = rs.getMetaData();
		int count = md.getColumnCount();
		for(int i=0;i<count; i++) {
			System.out.println("i["+i+"]: "+md.getColumnName(i+1));
		}
		rs.close();
	}

	@Test
	public void testFK() throws IntrospectionException, SQLException {
		List<FK> list = new ArrayList<FK>();
		FK fk = new FK();
		list.add(fk);
		
		ResultSet rs = new ResultSetListAdapter<FK>("fks", statusUniqueColumns, list, FK.class);
		ResultSetMetaData md = rs.getMetaData();
		int count = md.getColumnCount();
		for(int i=0;i<count; i++) {
			System.out.println("i["+i+"]: "+md.getColumnName(i+1));
		}
		rs.close();
	}
}
