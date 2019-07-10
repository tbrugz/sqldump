package tbrugz.sqldump.dbmodel;

public abstract class AbstractConstraint extends DBIdentifiable implements RemarkableDBObject {

	String remarks;

	@Override
	public String getRemarks() {
		return remarks;
	}

	@Override
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

}
