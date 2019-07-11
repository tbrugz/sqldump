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

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class BaseResultSetCollectionAdapter<E extends Object> extends AbstractResultSet {
	
	static final Log log = LogFactory.getLog(BaseResultSetCollectionAdapter.class);

	static String collectionValuesJoiner = null;
	
	final String name;
	final List<String> columnNames;
	final List<Integer> columnTypes; // not (yet) used
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
		columnTypes = new ArrayList<Integer>();
		
		//Class<E> clazz = (Class<E>) value.getClass();
		
		BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		if(uniqueCols!=null) {
			for(String col: uniqueCols) {
				addMatchProperties(clazz, propertyDescriptors, col, columnNames, columnTypes, true);
			}
		}
		if(!onlyUniqueCols) {
			if(allCols!=null) {
				{
					List<String> commonCols = new ArrayList<String>();
					commonCols.addAll(uniqueCols);
					commonCols.retainAll(allCols);
					if(commonCols.size()>0) {
						log.warn("uniqueCols & xtraCols have ["+commonCols.size()+"] columns in common: "+commonCols);
					}
				}
				
				for(String col: allCols) {
					addMatchProperties(clazz, propertyDescriptors, col, columnNames, columnTypes, true);
				}
			}
			else {
				// add all!
				addMatchProperties(clazz, propertyDescriptors, null, columnNames, columnTypes, false);
			}
		}
		else {
			if(allCols!=null) {
				log.warn("onlyUniqueCols is true but allCols not null: "+allCols);
			}
		}
		
		metadata = new RSMetaDataTypedAdapter(null, name, columnNames, columnTypes);
		
		log.debug("resultset: cols: "+columnNames+" ; types: "+columnTypes); //+" ; methods: "+methods);
	}
	
	int addMatchProperties(Class<?> clazz, PropertyDescriptor[] propertyDescriptors, String matchCol, List<String> columnNames, List<Integer> columnTypes, boolean allowClassProperty) {
		int matched = 0;
		for (PropertyDescriptor prop : propertyDescriptors) {
			String pname = prop.getName();
			if(matchCol==null || matchCol.equals(pname)) {
				if(!allowClassProperty && "class".equals(pname)) { continue; }
				if(columnNames.contains(pname)) { continue; }
				//XXX: continue on transient, ... ??
				
				Method m = prop.getReadMethod();
				if(m==null) {
					log.warn("null get method? prop: "+pname+" class: "+clazz.getSimpleName());
					continue;
				}
				int ptype = SQLUtils.getSqlTypeFromClass(m.getReturnType());
				columnNames.add(pname);
				columnTypes.add(ptype);
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

	static String objectToString(Object obj) {
		if(obj==null) { return null; }
		if(collectionValuesJoiner!=null && obj instanceof Collection) {
			return Utils.join((Collection<?>) obj, collectionValuesJoiner);
		}
		else {
			return String.valueOf(obj);
		}
	}
	
	public static void setCollectionValuesJoiner(String collectionValuesJoiner) {
		BaseResultSetCollectionAdapter.collectionValuesJoiner = collectionValuesJoiner;
	}
	
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		Object ret = null;
		Method m = null;
		try {
			if(methods.size()>=columnIndex) {
				m = methods.get(columnIndex-1);
			}
			else {
				throw new IndexOutOfBoundsException("index ["+columnIndex+"]> methods.size() ["+methods.size()+"]");
			}
			if(m==null) {
				log.warn("method is null ["+(columnIndex-1)+"/"+methods.size()+"]");
				throw new IndexOutOfBoundsException("method index "+columnIndex+" not found");
				//return null;
			}
			return m.invoke(currentElement, (Object[]) null);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			log.warn("getObject[IllegalArgumentException]: method: "+m+" ; elem: "+currentElement+" ; ex: "+e); 
			//e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IndexOutOfBoundsException e) {
			//log.warn("getObject[IndexOutOfBoundsException]: "+(columnIndex-1)+" / size=="+methods.size());
			//e.printStackTrace();
			//throw e;
		}
		return ret;
	}
	
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		int index = columnNames.indexOf(columnLabel) + 1;
		//log.info("colnames: "+columnNames+" / label: "+columnLabel+" / index: "+index);
		if(index==0) { return null; }
		return getObject(index);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return objectToString(getObject(columnIndex));
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return objectToString(getObject(columnLabel));
	}
	
	@Override
	public int getInt(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).intValue();
		}
		return super.getInt(columnIndex);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		if(o instanceof Number) {
			return ((Number) o).intValue();
		}
		return super.getInt(columnLabel);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).longValue();
		}
		return super.getLong(columnIndex);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		if(o instanceof Number) {
			return ((Number) o).longValue();
		}
		return super.getLong(columnLabel);
	}
	
	@Override
	public float getFloat(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).floatValue();
		}
		return super.getFloat(columnIndex);
	}
	
	@Override
	public float getFloat(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		if(o instanceof Number) {
			return ((Number) o).floatValue();
		}
		return super.getFloat(columnLabel);
	}

	
	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).doubleValue();
		}
		return super.getDouble(columnIndex);
	}
	
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		if(o instanceof Number) {
			return ((Number) o).doubleValue();
		}
		return super.getDouble(columnLabel);
	}
	
}
