package tbrugz.sqldump.datadump.parquet;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import tbrugz.sqldump.datadump.DumpSyntaxBuilder;
import tbrugz.sqldump.datadump.OutputStreamDumper;
import tbrugz.sqldump.util.SQLUtils;

/*
 * see: https://avro.apache.org/docs/++version++/specification/
 * 
 * alternative: https://github.com/jvdsandt/Laminate
 */
public class ParquetSyntax extends OutputStreamDumper implements DumpSyntaxBuilder, Cloneable {

	public static final String PARQUET_SYNTAX_ID = "parquet"; // .pqt? change getDefaultFileExtension()?
	
	/*
	 * https://github.com/apache/parquet-format/issues/381
	 * application/vnd.apache.parquet ; application/parquet ; application/x-parquet ; application/octet-stream?
	 */
	public static final String MIME_TYPE = "application/vnd.apache.parquet"; 
	
	ParquetWriter<GenericRecord> writer;
	GenericRecord record;
	
	@Override
	public void procProperties(Properties prop) {
	}

	@Override
	public String getSyntaxId() {
		return PARQUET_SYNTAX_ID;
	}
	
	@Override
	public String getMimeType() {
		return MIME_TYPE;
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	static String getAvroSchema(String name, List<String> lsColNames, List<Class<?>> lsColTypes) {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n  \"type\": \"record\",\n  \"name\": \""+name+"\",\n  \"fields\": [");
		for(int i=0;i<lsColNames.size();i++) {
			String colName = lsColNames.get(i);
			Class<?> c = lsColTypes.get(i);
			String avroType = getAvroType(c);
			if(i>0) { sb.append(","); };
			sb.append("\n    {\"name\": \""+colName+"\", "+avroType+"}");
		}
		sb.append("\n  ]\n}");
		return sb.toString();
	}
	
	static String getAvroType(Class<?> clazz) {
		// see: SQLUtils.getClassFromSqlType
		if(clazz.equals(Integer.class)) { return makeType("long"); }
		if(clazz.equals(Double.class)) { return makeType("double"); }
		//if(clazz.equals(Date.class)) { return makeType("int", "date"); } //XXX
		if(clazz.equals(Date.class)) { return makeType("string"); }
		if(clazz.equals(String.class)) { return makeType("string"); }
		if(clazz.equals(Boolean.class)) { return makeType("boolean"); }
		if(clazz.equals(Blob.class)) { return makeType(null); }
		if(clazz.equals(Array.class)) { return makeType(null); }
		if(clazz.equals(ResultSet.class)) { return makeType(null); }
		if(clazz.equals(Object.class)) { return makeType("string"); }
		return makeType("string");
	}
	
	static String makeType(String type) {
		return makeType(type, null);
	}
	
	static String makeType(String type, String logicalType) {
		return "\"type\": \""+type+"\""+
			(logicalType!=null?", \"logicalType\": \""+logicalType+"\"":"");
	}

	@Override
	public void dumpHeader(OutputStream os) throws IOException {
		Schema schema = new Schema.Parser().parse( getAvroSchema(tableName, lsColNames, lsColTypes) );
		StreamOutputFile sof = new StreamOutputFile(os);
		writer = AvroParquetWriter.<GenericRecord>builder(sof)
				.withSchema(schema)
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.build();
		record = new GenericData.Record(schema);
	}

	@Override
	public void dumpRow(ResultSet rs, long count, OutputStream os)
			throws IOException, SQLException {
		boolean canReturnResultSet = false; //XXX: internal resultSet as record??
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, canReturnResultSet);
		for(int i=0;i<numCol;i++) {
			record.put(lsColNames.get(i), vals.get(i));
		}
		writer.write(record);
	}
	
	@Override
	public void dumpFooter(long count, boolean hasMoreRows, OutputStream os) throws IOException {
		writer.close();
	}

}
