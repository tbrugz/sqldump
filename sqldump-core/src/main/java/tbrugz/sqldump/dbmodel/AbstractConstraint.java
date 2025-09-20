package tbrugz.sqldump.dbmodel;

public abstract class AbstractConstraint extends DBIdentifiable implements RemarkableDBObject, ValidatableDBObject {

	String remarks;
	Boolean valid;

	@Override
	public String getRemarks() {
		return remarks;
	}

	@Override
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	@Override
	public Boolean getValid() {
		return valid;
	}

	@Override
	public void setValid(Boolean valid) {
		this.valid = valid;
	}
	
	@Override
	public boolean hasRemarks() {
		return remarks!=null;
	}

}
