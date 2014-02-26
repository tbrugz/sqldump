package tbrugz.sqldiff.model;

import java.util.List;

import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.NamedDBObject;

public class GrantDiff implements Diff, Comparable<GrantDiff> {
	
	final Grant grant;
	final ColumnDiff.NamedTable namedTable;
	final boolean isRevoke;
	
	public GrantDiff(Grant grant, String schemaName, boolean isRevoke) {
		this.grant = grant;
		this.isRevoke = isRevoke;
		this.namedTable = new ColumnDiff.NamedTable(schemaName, grant.table);
	}

	@Override
	public ChangeType getChangeType() {
		return isRevoke?ChangeType.DROP:ChangeType.ADD;
	}

	@Override
	public String getDiff() {
		return 
			(isRevoke?"revoke ":"grant ")
			+grant.privilege
			+" on "+namedTable.getName()
			+" to "+grant.grantee;
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
		return new GrantDiff(grant, namedTable.schemaName, !isRevoke);
	}
	
	@Override
	public int compareTo(GrantDiff o) {
		//int compare = grant.compareTo(o.grant);
		//if(compare!=0) return compare;
		
		int compare = grant.table.compareTo(o.grant.table);
		if(compare!=0) return compare;
		
		compare = grant.privilege.compareTo(o.grant.privilege);
		if(compare!=0) return compare;
		
		compare = grant.grantee.compareTo(o.grant.grantee);
		if(compare!=0) return compare;
		
		compare = grant.withGrantOption==o.grant.withGrantOption?0:1;
		if(compare!=0) return compare;
		
		return isRevoke==o.isRevoke?0:1;
	}

}
