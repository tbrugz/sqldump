package tbrugz.sqldiff.model;

import java.util.List;

import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.PrivilegeType;

public class GrantDiff implements Diff, Comparable<GrantDiff> {

	// grant properties
	final String schemaName;
	final String tableName;
	final PrivilegeType privilege;
	final String grantee;
	final boolean withGrantOption;

	// grant "diff" properties
	final boolean revoke;
	//final transient Grant grant;
	final transient ColumnDiff.NamedTable namedTable;
	
	public GrantDiff(String schemaName, String tableName, PrivilegeType privilege, String grantee, boolean withGrantOption, boolean isRevoke) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.privilege = privilege;
		this.grantee = grantee;
		this.withGrantOption = withGrantOption;
		//this.grant = grant;
		this.revoke = isRevoke;
		this.namedTable = new ColumnDiff.NamedTable(schemaName, tableName);
	}
	
	public GrantDiff(Grant grant, String schemaName, boolean isRevoke) {
		this(schemaName, grant.table, grant.privilege, grant.grantee, grant.withGrantOption, isRevoke);
		/*this.schemaName = schemaName;
		this.tableName = grant.table;
		this.privilege = grant.privilege;
		this.grantee = grant.grantee;
		this.withGrantOption = grant.withGrantOption;
		//this.grant = grant;
		this.isRevoke = isRevoke;
		this.namedTable = new ColumnDiff.NamedTable(schemaName, tableName);*/
	}

	@Override
	public ChangeType getChangeType() {
		return revoke?ChangeType.DROP:ChangeType.ADD;
	}

	@Override
	public String getDiff() {
		return 
			(revoke?"revoke ":"grant ")
			+privilege
			+" on "+namedTable.getName()
			+" to "+grantee;
			//+";\n\n";
	}

	@Override
	public List<String> getDiffList() {
		return DiffUtil.singleElemList( getDiff() );
	}

	@Override
	public DBObjectType getObjectType() {
		return DBObjectType.GRANT;
	}

	@Override
	public NamedDBObject getNamedObject() {
		return namedTable;
	}

	@Override
	public Diff inverse() {
		return new GrantDiff(schemaName, tableName, privilege, grantee, withGrantOption, !revoke);
	}
	
	@Override
	public int compareTo(GrantDiff o) {
		//int compare = grant.compareTo(o.grant);
		//if(compare!=0) return compare;
		
		int compare = tableName.compareTo(o.tableName);
		if(compare!=0) return compare;
		
		compare = privilege.compareTo(o.privilege);
		if(compare!=0) return compare;
		
		compare = grantee.compareTo(o.grantee);
		if(compare!=0) return compare;
		
		compare = withGrantOption==o.withGrantOption?0:1;
		if(compare!=0) return compare;
		
		return revoke==o.revoke?0:1;
	}

}
