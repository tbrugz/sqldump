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
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class BaseResultSetCollectionAdapter<E extends Object> extends AbstractResultSet {
	
	static final Log log = LogFactory.getLog(BaseResultSetCollectionAdapter.class);

	static String collectionValuesJoiner = null;
	
	final String name;
	final List<String> columnNames;
	final ResultSetMetaData metadata;
	final List<Method> methods = new ArrayList<Method>();

	E currentElement;

	public BaseResultSetCollectionAdapter(String name, List<String> uniqueCols, Class<? extends E> clazz) throws IntrospectionException {
		this(name, uniqueCols, null, false, clazz);
	}
	
	public BaseResultSetCollectionAdapter(String name, List<String> uniqueCols, List<String> allCols, Class<? extends E> clazz) throws IntrospectionException {
		this(name, uniqueCols, allCols, false, clazz);
	}

	public BaseResultSetCollectionAdapter(String name, List<String> uniqueCols, List<String> allCols, boolean onlyUniqueCols, Class<? extends E> clazz) throws IntrospectionException {
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
				// add all!
				addMatchProperties(clazz, propertyDescriptors, null, columnNames);
			}
		}
		log.debug("resultset:cols: "+columnNames);
	}
	
	int addMatchProperties(Class<?> clazz, PropertyDescriptor[] propertyDescriptors, String matchCol, List<String> columnNames) {
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
				if(matchCol!=null) { return 1; }
				matched++;
			}
		}
		if(matched==0) {
			log.warn("column '"+matchCol+"' not matched: missing a getter? [class: "+clazz.getSimpleName()+"]");
		}
		return matched;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		String ret = null;
		Method m = null;
		try {
			if(methods.size()>=columnIndex) {
				m = methods.get(columnIndex-1);
			}
			//else {throw new IndexOutOfBoundsException} ?
			if(m==null) {
				log.warn("method is null ["+(columnIndex-1)+"/"+methods.size()+"]");
				throw new IndexOutOfBoundsException("method index "+columnIndex+" not found");
				//return null;
			}
			Object oret = m.invoke(currentElement, (Object[]) null);
			if(oret==null) { return null; }
			if(collectionValuesJoiner!=null && oret instanceof Collection) {
				ret = Utils.join((Collection<?>) oret, collectionValuesJoiner);
			}
			else {
				ret = String.valueOf(oret);
			}
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			log.warn("method: "+m+" ; elem: "+currentElement+" ; ex: "+e); 
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
	
	public static void setCollectionValuesJoiner(String collectionValuesJoiner) {
		BaseResultSetCollectionAdapter.collectionValuesJoiner = collectionValuesJoiner;
	}
}
