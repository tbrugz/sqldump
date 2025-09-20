package tbrugz.sqldump.dbmodel;

public interface BodiedObject {

	public boolean isDumpable();

	public String getBody();

	public void setBody(String body);

}
