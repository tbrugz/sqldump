package tbrugz.util;

public class LongFactory extends GenericFactory<Long> {
	long initialValue = 0l;
	
	@Override
	public Long getInstance() {
		return initialValue;
	}
}
