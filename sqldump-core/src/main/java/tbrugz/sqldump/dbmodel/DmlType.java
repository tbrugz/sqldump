package tbrugz.sqldump.dbmodel;

/**
 * see also:
 * dbmodel.PrivilegeType
 * https://en.wikipedia.org/wiki/Data_manipulation_language
 */
public enum DmlType {
	SELECT, INSERT, UPDATE, DELETE,
	//XXX: ANY or ALL?
	//XXX: MERGE / UPSERT ?
	// VALUES? TRUNCATE?
	
	// Oracle: CALL, EXPLAIN PLAN, LOCK TABLE - http://www.orafaq.com/faq/what_are_the_difference_between_ddl_dml_and_dcl_commands
	// Postgresql: VALUES - http://www.postgresql.org/docs/9.5/static/sql-values.html
	// pgsql: COPY - http://www.postgresql.org/docs/9.2/static/sql-copy.html
}
