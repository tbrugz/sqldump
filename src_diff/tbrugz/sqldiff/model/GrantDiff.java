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
	final ChangeType changeType;
	//final transient Grant grant;
	final transient ColumnDiff.NamedTable namedTable;
	
	public GrantDiff(String schemaName, String tableName, PrivilegeType privilege, String grantee, boolean withGrantOption, boolean revoke) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.privilege = privilege;
		this.grantee = grantee;
		this.withGrantOption = withGrantOption;
		//this.grant = grant;
		this.changeType = revoke?ChangeType.DROP:ChangeType.ADD;
		this.namedTable = new ColumnDiff.NamedTable(schemaName, tableName);
	}
	
	public GrantDiff(Grant grant, String schemaName, boolean isRevoke) {
		this(schemaName, grant.getTable(), grant.getPrivilege(), grant.getGrantee(), grant.isWithGrantOption(), isRevoke);
	}

	@Override
	public ChangeType getChangeType() {
		return changeType;
	}

	@Override
	public String getDiff() {
		return 
			(changeType.equals(ChangeType.DROP)?"revoke ":"grant ")
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
		return new GrantDiff(schemaName, tableName, privilege, grantee, withGrantOption, changeType.equals(ChangeType.ADD)?true:false);
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
		
		return changeType.compareTo(o.changeType);
	}
	
	@Override
	public String getDefinition() {
		return changeType.equals(ChangeType.ADD)?getDiff():"";
	}
	
	@Override
	public String getPreviousDefinition() {
		return changeType.equals(ChangeType.DROP)?inverse().getDiff():"";
	}

}
