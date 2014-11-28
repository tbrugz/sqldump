package tbrugz.sqldump.resultset;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BaseResultSetCollectionAdapter<E extends Object> extends AbstractResultSet {
	
	static final Log log = LogFactory.getLog(BaseResultSetCollectionAdapter.class);

	final String name;
	final List<String> columnNames;
	final ResultSetMetaData metadata;
	final List<Method> methods = new ArrayList<Method>();

	E currentElement;

	public BaseResultSetCollectionAdapter(String name, List<String> uniqueCols, Class<E> clazz) throws IntrospectionException {
		this(name, uniqueCols, null, false, clazz);
	}
	
	public BaseResultSetCollectionAdapter(String name, List<String> uniqueCols, List<String> allCols, Class<E> clazz) throws IntrospectionException {
		this(name, uniqueCols, allCols, false, clazz);
	}

	public BaseResultSetCollectionAdapter(String name, List<String> uniqueCols, List<String> allCols, boolean onlyUniqueCols, Class<E> clazz) throws IntrospectionException {
		this.name = name;
		
		columnNames = new ArrayList<String>();
		metadata = new RSMetaDataAdapter(null, name, columnNames);
		
		//Class<E> clazz = (Class<E>) value.getClass();
		
		BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		if(uniqueCols!=null) {
			for(String col: uniqueCols) {
				addMatchProperties(clazz, propertyDescriptors, col, columnNames);
			}
		}
		if(!onlyUniqueCols) {
			if(allCols!=null) {
				for(String col: allCols) {
					addMatchProperties(clazz, propertyDescriptors, col, columnNames);
				}
			}
			else {
				addMatchProperties(clazz, propertyDescriptors, null, columnNames);
			}
		}
		log.debug("resultset:cols: "+columnNames);
	}
	
	void addMatchProperties(Class<?> clazz, PropertyDescriptor[] propertyDescriptors, String matchCol, List<String> columnNames) {
		int matched = 0;
		for (PropertyDescriptor prop : propertyDescriptors) {
			if(matchCol==null || matchCol.equals(prop.getName())) {
				String pname = prop.getName();
				if("class".equals(pname)) { continue; }
				if(columnNames.contains(pname)) { continue; }
				//XXX: continue on transient, ... ??
				
				Method m = prop.getReadMethod();
				if(m==null) {
					log.warn("null get method? prop: "+pname+" class: "+clazz.getSimpleName());
					continue;
				}
				columnNames.add(pname);
				methods.add(m);
				if(matchCol!=null) { return; }
				matched++;
			}
		}
		if(matched==0) {
			log.warn("column '"+matchCol+"' not matched: missing a getter? [class: "+clazz.getSimpleName()+"]");
		}
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		String ret = null;
		try {
			Method m = methods.get(columnIndex-1);
			if(m==null) {
				log.warn("method is null ["+(columnIndex-1)+"/"+methods.size()+"]");
				return null;
			}
			Object oret = m.invoke(currentElement, (Object[]) null);
			if(oret==null) { return null; }
			ret = String.valueOf(oret);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} /*catch (IndexOutOfBoundsException e) {
			e.printStackTrace();
		}*/
		return ret;
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		int index = columnNames.indexOf(columnLabel) + 1;
		//log.info("colnames: "+columnNames+" / label: "+columnLabel+" / index: "+index);
		if(index==0) { return null; }
		return getString(index);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}
	
}
