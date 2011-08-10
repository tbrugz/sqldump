package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SQLUtils;

//XXXdone: left-align for strings & right-align for numbers
//XXX: prop for 'separator' & 'lineGroupSize'

//FFC: Formatted Fixed Column
public class FFCDataDump extends DumpSyntax {

	static final String FFC_SYNTAX_ID = "ffc";
	static Logger log = Logger.getLogger(FFCDataDump.class);
	
	//String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	
	
	@Override
	public void procProperties(Properties prop) {
	}

	@Override
	public void initDump(String tableName, ResultSetMetaData md) throws SQLException {
		//this.tableName = tableName;
		numCol = md.getColumnCount();		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getScale(i+1)));
			
			if(Number.class.isAssignableFrom(lsColTypes.get(i))) {
				leftAlignField.add(false);
			}
			else {
				leftAlignField.add(true);
			}
		}
		//for(int i=0;i<lsColNames.size();i++) {
		//	log.debug("col: "+lsColNames.get(i)+"/"+lsColNames.get(i).length());
		//	headersColsMaxLenght.add(lsColNames.get(i).length());
		//}
		clearBuffer();
		//colsMaxLenght.addAll(headersColsMaxLenght);
	}
	
	int lineGroupSize = 20;
	String separator = " | ";

	//List<Integer> headersColsMaxLenght = new ArrayList<Integer>();
	List<Integer> colsMaxLenght = new ArrayList<Integer>();
	List<List<String>> valuesBuffer = new ArrayList<List<String>>();
	List<Boolean> leftAlignField = new ArrayList<Boolean>();
	
	@Override
	public void dumpHeader(Writer fos) throws Exception {
	}

	@Override
	public void dumpRow(ResultSet rs, int count, Writer fos) throws Exception {
		if(count!=0 && count%lineGroupSize==0) {
			dumpBuffer(fos);
		}

		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColNames.size();i++) {
			int max = colsMaxLenght.get(i);
			Object value = vals.get(i);
			if(value!=null) {
				String valueStr = String.valueOf(value);
				if(max<valueStr.length()) { 
					colsMaxLenght.set(i, valueStr.length());
				}
			}
			int maxCol = lsColNames.get(i).length();
			if(maxCol>max) {
				colsMaxLenght.set(i, maxCol);
			}
		}
		valuesBuffer.add(vals);
	}
	
	void clearBuffer() {
		//clean up
		valuesBuffer.clear();
		colsMaxLenght.clear();
		//colsMaxLenght.addAll(headersColsMaxLenght);
		for(int i=0;i<lsColNames.size();i++) { colsMaxLenght.add(0); }
	}
	
	void dumpBuffer(Writer fos) throws IOException {
		//print buffer
		StringBuffer sb = new StringBuffer();
		for(int j=0;j<lsColNames.size();j++) {
			//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
			appendString(sb, colsMaxLenght.get(j), lsColNames.get(j), j);
		}
		sb.append("\n");
		for(int i=0;i<valuesBuffer.size();i++) {
			List<String> vals = valuesBuffer.get(i);
			for(int j=0;j<lsColNames.size();j++) {
				appendString(sb, colsMaxLenght.get(j), String.valueOf(vals.get(j)), j );
			}
			sb.append("\n");
		}
		out(sb.toString(), fos); //+"\n"
		
		clearBuffer();
	}
	
	void appendString(StringBuffer sb, int len, String value, int colIndex) {
		//sb.append( String.format("%"+len+"s"+separator, value) );
		if(leftAlignField.get(colIndex)) {
			sb.append( String.format("%-"+len+"s"+separator, value) );
		}
		else {
			sb.append( String.format("%"+len+"s"+separator, value) );
		}
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
		dumpBuffer(fos);
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(s);
	}
	
	@Override
	public String getSyntaxId() {
		return FFC_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "ffc.txt";
	}
}
