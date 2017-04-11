package mondrian.sampling;

import java.util.List;

/**
 * 该类描述数据库中的样本信息
 * 
 * @Description 系统中提供的样本信息
 * @author zoe
 * @date 2017年4月8日 上午8:20:24 
 *
 */
public class SampleInfo {
	private String originTable;
	private String sampleTable;
	private int samplingSchema;			//defined in SamplingSchema
	private float samplingRate;			//抽样率
	/*
	 * 分组属性：
	 * 基于单表的抽样：attr1, attr2...
	 * 基于多表的抽样：table1.attr1 ,table2.attr2...
	 */
	private List<String> groupingAttrs;			
	
	//基于多表抽样时独有的元信息
	private boolean multiTableSampling ;
	private List<String> joinAttrs;	
	
	
	
	public float getSamplingRate() {
		return samplingRate;
	}

	public void setSamplingRate(float samplingRate) {
		this.samplingRate = samplingRate;
	}

	public List<String> getGroupingAttrs() {
		return groupingAttrs;
	}

	public void setGroupingAttrs(List<String> groupingAttrs) {
		this.groupingAttrs = groupingAttrs;
	}

	public boolean isMultiTableSampling() {
		return multiTableSampling;
	}

	public void setMultiTableSampling(boolean multiTableSampling) {
		this.multiTableSampling = multiTableSampling;
	}

	public List<String> getJoinAttrs() {
		return joinAttrs;
	}

	public void setJoinAttrs(List<String> joinAttrs) {
		this.joinAttrs = joinAttrs;
	}

	public String getOriginTable() {
		return originTable;
	}

	public void setOriginTable(String originTable) {
		this.originTable = originTable;
	}

	public String getSampleTable() {
		return sampleTable;
	}

	public void setSampleTable(String sampleTable) {
		this.sampleTable = sampleTable;
	}

	public int getSamplingSchema() {
		return samplingSchema;
	}

	public void setSamplingSchema(int samplingSchema) {
		this.samplingSchema = samplingSchema;
	}

	
	
	
}
