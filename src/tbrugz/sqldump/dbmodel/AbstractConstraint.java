package tbrugz.sqldump.dbmodel;

public abstract class AbstractConstraint extends DBIdentifiable {

	String remarks;

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

}
