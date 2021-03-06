package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.util.StringUtils;

public abstract class PostgreSQLAbstractFeatures extends InformationSchemaFeatures {

	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(PostgreSQLAbstractFeatures.class);

	// org.postgresql.jdbc4.Jdbc4DatabaseMetaData.getFunction(String, String, String) not implemented
	//static final DBObjectType[] execTypes = new DBObjectType[]{ DBObjectType.FUNCTION, DBObjectType.PROCEDURE };
	
	@Override
	public boolean supportsDiffingColumn() {
		return true;
	}

	/*
	@Override
	public String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn,
			Column column) {
		//diff type, if no diff: diff default, if no diff: diff nullable
		//see: http://www.postgresql.org/docs/9.1/static/sql-altertable.html
		if(! StringUtils.equalsWithUpperCase(previousColumn.getTypeDefinition(), column.getTypeDefinition())) {
			return DiffUtil.createAlterColumn(this, table, column,
					" set data type "+column.getTypeDefinition());
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			return DiffUtil.createAlterColumn(this, table, column,
					(column.getDefaultSnippet().trim().equals("")?" drop default":" set"+column.getDefaultSnippet()));
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			return DiffUtil.createAlterColumn(this, table, column,
					(column.isNullable()?" drop":" set")+" not null");
		}

		log.warn("no differences between PostgreSQL columns found");
		return null;
		//throw new UnsupportedOperationException("no differences between PostgreSQL columns found");
	}
	*/
	
	@Override
	public String sqlAlterColumnByDiffing(Column previousColumn, Column column) {
		//diff type, if no diff: diff default, if no diff: diff nullable
		//see: http://www.postgresql.org/docs/9.1/static/sql-altertable.html
		if(! StringUtils.equalsWithUpperCase(previousColumn.getTypeDefinition(), column.getTypeDefinition())) {
			return " set data type "+column.getTypeDefinition();
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			return (column.getDefaultSnippet().trim().equals("")?" drop default":" set"+column.getDefaultSnippet());
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			return (column.isNullable()?" drop":" set")+" not null";
		}

		//log.warn("no differences between PostgreSQL columns found");
		return null;
		//throw new UnsupportedOperationException("no differences between PostgreSQL columns found");
	}
	
	/*@Override
	public List<DBObjectType> getExecutableObjectTypes() {
		return Arrays.asList(execTypes);
	}
	*/
	
	@Override
	public List<DBObjectType> getSupportedObjectTypes() {
		List<DBObjectType> ret = new ArrayList<DBObjectType>();
		ret.addAll(super.getSupportedObjectTypes());
		ret.add(DBObjectType.MATERIALIZED_VIEW);
		return ret;
	}
	
	@Override
	public boolean supportsCreateIndexWithoutName() {
		return true;
	}
	
	/*
	 * see: https://dba.stackexchange.com/questions/27963/postgres-requires-commit-or-rollback-after-exception
	 */
	@Override
	public boolean sqlExceptionRequiresRollback() {
		return true;
	}
	
	@Override
	public String sqlIsNullFunction(String columnName) {
		return columnName+" is null";
	}

	@Override
	public String sqlLengthFunctionByType(String columnName, String columnType) {
		//String atype = DBMSResources.instance().toANSIType(getId(), columnType);
		//if(atype==null) { return null; }
		//atype = atype.toUpperCase();
		//log.info("sqlLengthFunctionByType: columnType = "+columnType);
		if(Column.ColTypeUtil.isCharacter(columnType) || Column.ColTypeUtil.isBinary(columnType)) {
			return "length("+columnName+")";
		}
		if(Column.ColTypeUtil.isNumeric(columnType) || Column.ColTypeUtil.isDatetime(columnType) || Column.ColTypeUtil.isBoolean(columnType)) {
			return "length("+columnName+"::text)";
		}
		return null;
	}
	
}
