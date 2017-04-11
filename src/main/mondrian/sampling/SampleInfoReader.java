package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import db.DBUtil;

/**
 * 
 * @Description 从数据库读取样本元信息，被其他类使用来共享样本信息
 * @author zoe
 * @date 2017年4月8日 上午9:21:06 
 *
 */
public class SampleInfoReader {
	
	public static List<SampleInfo> sampleInfos ;
	
	static {
		sampleInfos = new ArrayList<SampleInfo>();
	}
	
	public SampleInfoReader(){
		
	}
	/**
	 * 从数据库加载样本元信息保存到smapleInfo中
	 */
	public static void loadSampleInfo(){
		DBUtil dbUtil = new DBUtil();
		StringBuilder sampleInfoQuery = new StringBuilder();
		sampleInfoQuery.append("SELECT * FROM sample_info");
		
		ResultSet rs = null;
		rs = dbUtil.excuteQuery(sampleInfoQuery.toString());
		
		try {
			while(rs.next()){
				String originTable = rs.getString("origin_table");
				String sampleTable = rs.getString("sample_table");
				int samplingSchema = rs.getInt("samplingSchema");
				boolean multiTableSampling = rs.getInt("multi_table_sampling") == 0 ? false:true;
				String joinAttrs = rs.getString("join_attrs");
				String groupingAttrs = rs.getString("grouping_attrs");
				float samplingRate = rs.getFloat("samplingRate");
				
				SampleInfo sampleInfo = new SampleInfo();
				sampleInfo.setOriginTable(originTable);
				sampleInfo.setSampleTable(sampleTable);
				sampleInfo.setSamplingRate(samplingRate);
				sampleInfo.setSamplingSchema(samplingSchema);
				sampleInfo.setGroupingAttrs(Arrays.asList(groupingAttrs.split("\\.")));
				sampleInfo.setMultiTableSampling(multiTableSampling);
				sampleInfo.setJoinAttrs(Arrays.asList(joinAttrs.split("\\.")));
				sampleInfos.add(sampleInfo);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void test(){
		SampleInfoReader.loadSampleInfo();
	}

}
