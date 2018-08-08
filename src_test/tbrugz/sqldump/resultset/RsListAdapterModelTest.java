package tbrugz.sqldump.resultset;

import java.beans.IntrospectionException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.util.SQLUtils;

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
	
	@Test
	public void testBeanWithList() throws IntrospectionException, SQLException {
		BeanWithList b1 = new BeanWithList(1, Arrays.asList(new String[] {"one", "uno", "ein"}));
		BeanWithList b2 = new BeanWithList(2, Arrays.asList(new String[] {"two", "due", "zwei"}));
		
		List<BeanWithList> list = new ArrayList<BeanWithList>();
		list.add(b1); list.add(b2);
		
		ResultSet rs = new ResultSetListAdapter<BeanWithList>("beans", BeanWithList.getUniqueCols(), BeanWithList.getAllCols(), list, BeanWithList.class);
		ResultSetMetaData md = rs.getMetaData();
		int colCount = md.getColumnCount();
		// columns
		/*for(int i=0;i<colCount; i++) {
			System.out.println("i["+i+"]: "+md.getColumnName(i+1)+" / "+md.getColumnType(i+1));
		}*/
		
		List<String> lsColNames = new ArrayList<String>();
		List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
		for(int i=0;i<colCount;i++) {
			lsColNames.add(md.getColumnLabel(i+1));
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		//System.out.println("column names: "+lsColNames+" ; column types: "+lsColTypes+"\n---");
		
		// data
		while(rs.next()) {
			List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, colCount, true);
			//System.out.println("vals: "+);
			/*for(int i=0;i<vals.size(); i++) {
				Object o = vals.get(i);
				Class<?> c = lsColTypes.get(i);
				boolean isArray = DataDumpUtils.isArray(c, o);
				System.out.print(o+" ["+o.getClass()+"]["+c+"]["+isArray+"]; ");
			}
			System.out.println();*/

			// col "id"
			{
				Object o0 = vals.get(0);
				Class<?> c0 = lsColTypes.get(0);
				Assert.assertEquals(Integer.class, c0);
				Assert.assertFalse(DataDumpUtils.isArray(c0, o0));
			}
			// col "names"
			{
				Object o1 = vals.get(1);
				Class<?> c1 = lsColTypes.get(1);
				Assert.assertEquals(java.sql.Array.class, c1);
				Assert.assertTrue(DataDumpUtils.isArray(c1, o1));
			}
			
			/*for(int i=0;i<colCount; i++) {
				Object o = rs.getObject(i+1);
				System.out.print(o+" ["+o.getClass()+"]; ");
			}
			System.out.println("\n---");*/
		}
		rs.close();
	}
	
}
