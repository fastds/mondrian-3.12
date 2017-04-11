package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import db.DBUtil;

public class CongressionalSamplesTest {
	
	private String originTable = "inventory_fact_1997";
	private String sampleTable = "inventory_fact_1997_congress_with_join";
	private float samplingRate = 0.07f;
	private String aggregateAttr = "store_invoice";
	
	@Test
	public void testSampling(){
		CongressionalSampling cs = new CongressionalSampling();
		cs.house();
		cs.senate();
		cs.basicCongress();
		cs.congress();
		cs.materializeCongress();
		cs.materializeBasicCongress();
		cs.materializeHouse();
		cs.materializeSenate();
	}
	@Test
	public void testSampling2(){
		List<String> groupingAttrs = new ArrayList<String>();
		groupingAttrs.add("time_id");
		groupingAttrs.add("warehouse_id");
		
		CongressionalSampling cs = new CongressionalSampling(originTable,sampleTable,(ArrayList)groupingAttrs,samplingRate);
		cs.house();
		cs.senate();
		cs.basicCongress();
		cs.congress();
		cs.materializeCongress();
//		cs.materializeBasicCongress();
//		cs.materializeHouse();
//		cs.materializeSenate();
	}
	
	@Test
	public void testSamplingWithJoin(){
		
		List<String> groupingAttrs = new ArrayList<String>();
		groupingAttrs.add("time_by_day.the_year");
		groupingAttrs.add("store.store_state");
		
		Map<String,String> mappingAttrs = new HashMap<String,String>();
		mappingAttrs.put("inventory_fact_1997.time_id", "time_by_day.time_id");
		mappingAttrs.put("inventory_fact_1997.store_id", "store.store_id");
		
		
		CongressionalSampling cs = new CongressionalSampling(originTable,sampleTable,(ArrayList)groupingAttrs,mappingAttrs,samplingRate);
		cs.house();
		cs.senate();
		cs.basicCongress();
		cs.congress();
		cs.materializeCongressWithJoin();
	}
	
	/**
	 * 基于样本数据计算count聚集操作的误差
	 */
	@Test
	public void computeCongressError(){
		String sql = "SELECT avg("+aggregateAttr+") FROM "+sampleTable+" GROUP BY time_id ";
		String summarySql = "SELECT avg("+aggregateAttr+") FROM "+originTable+" GROUP BY  time_id";
		DBUtil dbOne = new DBUtil();
		DBUtil dbTwo = new DBUtil();
		ResultSet rsOne = dbOne.excuteQuery(sql);
		ResultSet rsTwo = dbTwo.excuteQuery(summarySql);
		float error = 0;
		try {
			int i = 0;
			while(rsOne.next() && rsTwo.next()){
				i++;
				float temp = Math.abs(rsOne.getFloat(1)-rsTwo.getFloat(1))/rsTwo.getFloat(1);
				error += temp;
				System.out.println("error "+i+" : "+temp*100);
			}
			System.out.println("average error:" + error/i*100);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 基于样本数据计算count聚集操作的误差
	 */
	@Test
	public void computeBasicCongressError(){
		String sql = "SELECT avg("+aggregateAttr+") FROM test_basic_congress_sample GROUP BY group_one ";
		String summarySql = "SELECT avg("+aggregateAttr+") FROM test GROUP BY  group_one";
		DBUtil dbOne = new DBUtil();
		DBUtil dbTwo = new DBUtil();
		ResultSet rsOne = dbOne.excuteQuery(sql);
		ResultSet rsTwo = dbTwo.excuteQuery(summarySql);
		float error = 0;
		try {
			int i = 0;
			while(rsOne.next() && rsTwo.next()){
				i++;
				float temp = Math.abs(rsOne.getFloat(1)-rsTwo.getFloat(1))/rsTwo.getFloat(1);
				error += temp;
				System.out.println("error "+i+" : "+temp*100);
			}
			System.out.println("average error:"+error/i*100);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 基于样本数据计算count聚集操作的误差
	 */
	@Test
	public void computeHouseError(){
		String sql = "SELECT avg("+aggregateAttr+") FROM test_house_sample GROUP BY group_one ";
		String summarySql = "SELECT avg("+aggregateAttr+") FROM test GROUP BY  group_one";
		DBUtil dbOne = new DBUtil();
		DBUtil dbTwo = new DBUtil();
		ResultSet rsOne = dbOne.excuteQuery(sql);
		ResultSet rsTwo = dbTwo.excuteQuery(summarySql);
		float error = 0 ;
		try {
			int i = 0;
			while(rsOne.next() && rsTwo.next()){
				i++;
				float temp = Math.abs(rsOne.getFloat(1)-rsTwo.getFloat(1))/rsTwo.getFloat(1);
				error += temp;
				System.out.println("error "+i+" : "+temp*100);
			}
			System.out.println("average error:"+error/i*100);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 基于样本数据计算count聚集操作的误差
	 * TODO 这里计算出来只有一组的误差？
	 */
	@Test
	public void computeSenateError(){
		String sql = "SELECT avg("+aggregateAttr+") FROM test_senate_sample GROUP BY group_one ";
		String summarySql = "SELECT avg("+aggregateAttr+") FROM test GROUP BY  group_one";
		DBUtil dbOne = new DBUtil();
		DBUtil dbTwo = new DBUtil();
		ResultSet rsOne = dbOne.excuteQuery(sql);
		ResultSet rsTwo = dbTwo.excuteQuery(summarySql);
		float error =  0;
		try {
			int i = 0;
			while(rsOne.next() && rsTwo.next()){
				i++;
				float temp = Math.abs(rsOne.getFloat(1)-rsTwo.getFloat(1))/rsTwo.getFloat(1);
				error += temp;
				System.out.println("error "+i+" : "+temp*100);
			}
			System.out.println("average error:"+error/i*100);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testSampleInfoWriter(){
		SampleInfo info = new SampleInfo();
		List<String> groupingAttrs = new ArrayList<String>();
		groupingAttrs.add("t2.a1");
		groupingAttrs.add("t3.a2");
		List<String> joinAttrs = new ArrayList<String>();
		joinAttrs.add("t1.j1:t2.j2");
		joinAttrs.add("t1.j3:t3.j4");
		info.setOriginTable("origin");
		info.setGroupingAttrs(groupingAttrs);
		info.setJoinAttrs(joinAttrs);
		info.setMultiTableSampling(false);
		info.setSampleTable("sample");
		info.setSamplingRate(0.01f);
		info.setSamplingSchema(0);
		SampleInfoWriter.write(info);
	}
	@Test
	public void test2(){
		float error = 100;
		float confidence = 0.9f;
		double hi = 12333.345;
		double lo = 12120.334;
		int sampleSize = 
				(int) Math.ceil(((hi-lo)*(hi-lo)*Math.log(2/(1-confidence)))/(2*error*error));
		System.out.println("sample Size:" + sampleSize);
		
	}
}
