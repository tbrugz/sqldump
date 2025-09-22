package tbrugz.sqldump.def;

public abstract class AbstractModelDumper extends AbstractFailable implements ProcessComponent {
	
	protected String processorId;
	
	@Override
	public void setId(String processorId) {
		this.processorId = processorId;
	}

	@Override
	public String getId() {
		return processorId;
	}

	public String getIdDesc() {
		return processorId!=null?"["+processorId+"] ":"";
	}
	
}
