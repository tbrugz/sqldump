package tbrugz.sqldump.dbmodel;

import java.util.Collection;

import javax.xml.bind.annotation.XmlTransient;

public abstract class DBIdentifiable {
	@XmlTransient
	public String name;
	//DBObjectType objtype;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	//XXX: getDefinition() should have 'sql dialect' param?
	public abstract String getDefinition(boolean dumpSchemaName);

	public static <T extends DBIdentifiable> T getDBIdentifiableByName(Collection<? extends DBIdentifiable> dbid, String colName) {
		for(DBIdentifiable d: dbid) {
			if(d.getName().equals(colName)) return (T) d;
		}
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBIdentifiable other = (DBIdentifiable) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
}
