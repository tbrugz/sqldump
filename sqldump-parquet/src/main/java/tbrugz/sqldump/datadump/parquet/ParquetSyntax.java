package tbrugz.sqldump.datadump.parquet;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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

	//private static final Log log = LogFactory.getLog(ParquetSyntax.class);

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
	
	static Schema getAvroSchema(String name, List<String> lsColNames, List<Class<?>> lsColTypes, List<Boolean> lsColNullables) {
		String avroSchema = getAvroSchemaString(name, lsColNames, lsColTypes, lsColNullables);
		//log.debug("avro schema: "+avroSchema);
		Schema schema = new Schema.Parser().parse( avroSchema );
		return schema;
	}
	
	static String getAvroSchemaString(String name, List<String> lsColNames, List<Class<?>> lsColTypes, List<Boolean> lsColNullables) {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n  \"type\": \"record\",\n  \"name\": \""+name+"\",\n  \"fields\": [");
		for(int i=0;i<lsColNames.size();i++) {
			String colName = lsColNames.get(i);
			Class<?> c = lsColTypes.get(i);
			boolean nullable = lsColNullables.get(i);
			String avroType = getAvroType(c, nullable);
			if(i>0) { sb.append(","); };
			sb.append("\n    {\"name\": \""+colName+"\", "+avroType+"}");
		}
		sb.append("\n  ]\n}");
		return sb.toString();
	}
	
	static String getAvroType(Class<?> clazz, boolean nullable) {
		// see: SQLUtils.getClassFromSqlType
		if(clazz.equals(Integer.class)) { return makeType("long", nullable); }
		if(clazz.equals(Double.class)) { return makeType("double", nullable); }
		if(clazz.equals(Date.class)) { return makeType("long", nullable, "timestamp-millis"); } // date?
		//if(clazz.equals(Date.class)) { return makeType("string", nullable); }
		if(clazz.equals(String.class)) { return makeType("string", nullable); }
		if(clazz.equals(Boolean.class)) { return makeType("boolean", nullable); }
		if(clazz.equals(Blob.class)) { return makeType(null, nullable); }
		if(clazz.equals(Array.class)) { return makeType(null, nullable); }
		if(clazz.equals(ResultSet.class)) { return makeType(null, nullable); }
		if(clazz.equals(Object.class)) { return makeType("string", nullable); }
		return makeType("string", nullable);
	}
	
	static String makeType(String type, boolean nullable) {
		return makeType(type, nullable, null);
	}
	
	static String makeType(String type, boolean nullable, String logicalType) {
		if(logicalType!=null) {
			if(nullable) {
				return "\"type\": [\"null\", {\"type\": \""+type+"\", \"logicalType\": \""+logicalType+"\" }]";
			}
			return "\"type\": {\"type\": \""+type+"\", \"logicalType\": \""+logicalType+"\" }";
		}
		if(nullable) {
			return "\"type\": [\"null\", \""+type+"\"]";
		}
		return "\"type\": \""+type+"\"";
	}
	
	static Object getAvroObject(Object o, Class<?> clazz) {
		if(clazz.equals(Date.class)) {
			if(o instanceof Timestamp) {
				Timestamp t = (Timestamp) o;
				return t.getTime();
			}
			throw new IllegalArgumentException("Incompatible classes: class "+clazz.getSimpleName()+", object of class "+o.getClass().getSimpleName());
		}
		return o;
	}

	@Override
	public void dumpHeader(OutputStream os) throws IOException {
		Schema schema = getAvroSchema(tableName, lsColNames, lsColTypes, lsColNullables);
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
			record.put(lsColNames.get(i), getAvroObject(vals.get(i), lsColTypes.get(i)));
		}
		writer.write(record);
	}
	
	@Override
	public void dumpFooter(long count, boolean hasMoreRows, OutputStream os) throws IOException {
		writer.close();
	}

	@Override
	public boolean requiresColumnNulability() {
		return true;
	}

}
