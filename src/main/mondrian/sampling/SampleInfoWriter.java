package mondrian.sampling;

import java.util.List;

import db.DBUtil;


/**
 * 
 * @Description 将新创建的样本写到元数据表中
 * @author zoe
 * @date 2017年4月11日 上午10:26:00 
 *
 */
public class SampleInfoWriter {
	
	public static void write(SampleInfo info){
		StringBuilder insertMeta = new StringBuilder();
		List<String> groupingAttrs = info.getGroupingAttrs();
		List<String> joinAttrs = info.getJoinAttrs();
		StringBuilder groupingAttrsStr = new StringBuilder();
		StringBuilder joinAttrsStr = new StringBuilder();
		int isMulti = info.isMultiTableSampling()?1:0;
		for(String attr : groupingAttrs){
			groupingAttrsStr.append(attr + ",");
		}
		
		for(String attr : joinAttrs){
			joinAttrsStr.append(attr + ",");
		}
		if(info.isMultiTableSampling()){
			insertMeta.append("insert into sample_info("
					+ "origin_table,sample_table"
					+ ", join_attrs, sampling_schema"
					+ ", grouping_attrs, multi_table_sampling"
					+ ", sampling_rate) ");
			insertMeta.append(" values('"
					+ info.getOriginTable()+"','"+info.getSampleTable()+"'"
					+ ",'"+joinAttrsStr.substring(0,joinAttrsStr.length()-1)+"',"+info.getSamplingSchema()
					+ ",'" + groupingAttrsStr.substring(0,groupingAttrsStr.length()-1) + "',"+isMulti
					+ ","+info.getSamplingRate()+")");
		}else{
			insertMeta.append("insert into sample_info("
					+ "origin_table,sample_table"
					+ ", sampling_schema, multi_table_sampling"
					+ ", sampling_rate) ");
			insertMeta.append(" values('"
					+ info.getOriginTable()+"','"+info.getSampleTable()+"'"
					+ ","+info.getSamplingSchema()+ "," +isMulti
					+ ","+info.getSamplingRate()+")");
		}
		System.out.println(insertMeta);
		new DBUtil().excute(insertMeta.toString());
	}
	
}
