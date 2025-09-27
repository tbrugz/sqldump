package tbrugz.sqldump.dbmodel;

import tbrugz.sqldiff.WhitespaceIgnoreType;
import tbrugz.sqldump.util.StringUtils;

public class Trigger extends DBObject implements BodiedObject {

	private static final long serialVersionUID = 1L;

	//TODO: add transient String name, add get/set
	String description;
	String body;
	String tableName;
	String whenClause;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return (dumpCreateOrReplace?"create or replace ":"create ") + "trigger "
				+ description + "\n"
				+ (whenClause!=null?"when ( "+whenClause.trim()+" )\n":"")
				+ body;
	}

	@Override
	public String toString() {
		return "[Trigger:"+description+"]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((body == null) ? 0 : body.hashCode());
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result
				+ ((whenClause == null) ? 0 : whenClause.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Trigger other = (Trigger) obj;
		if (body == null) {
			if (other.body != null)
				return false;
		} else if (!body.equals(other.body))
			return false;
		if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (whenClause == null) {
			if (other.whenClause != null)
				return false;
		} else if (!whenClause.equals(other.whenClause))
			return false;
		return true;
	}
	
	/* ignoring whitespaces */
	@Override
	public boolean equals4Diff(DBIdentifiable obj, WhitespaceIgnoreType wsIgnore) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Trigger other = (Trigger) obj;
		/* if (body == null) {
			if (other.body != null)
				return false;
		} else */
		if (!StringUtils.equalsIgnoreWhitespacesEachLine(body, other.body, wsIgnore))
		//} else if (!body.equals(other.body))
			return false;
		/* if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description)) */
		if (!StringUtils.equalsIgnoreWhitespacesEachLine(description, other.description, wsIgnore))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (whenClause == null) {
			if (other.whenClause != null)
				return false;
		} else if (!whenClause.equals(other.whenClause))
			return false;
		return true;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String getBody() {
		return body;
	}

	@Override
	public void setBody(String body) {
		this.body = body;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getWhenClause() {
		return whenClause;
	}

	public void setWhenClause(String whenClause) {
		this.whenClause = whenClause;
	}
	
	@Override
	public boolean isDumpable() {
		return body!=null;
	}

}
