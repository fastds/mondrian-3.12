package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import db.DBUtil;

/**
 * 
 * @Description 对Error-bounded抽样方法的测试
 * @author zoe
 * @date 2017年4月11日 下午6:27:26 
 *
 */
public class ErrorBoundedSamplingTest {
	String originTable = "inventory_fact_1997";
	String sampleTable = "inventory_fact_1997_error_bounded";
	@Test
	public void testErrorBoundedSampling(){
		
		List<String> groupingAttrs = new ArrayList<String>();
		groupingAttrs.add("store_id");
		
		List<String> aggregateAttrs = new ArrayList<String>();
		aggregateAttrs.add("store_invoice");
		
		float error = 5;
		float confidence = 0.9f;
		
		ErrorBoundedSampling sampling = new ErrorBoundedSampling(error, confidence, originTable, sampleTable, groupingAttrs, aggregateAttrs);
		Map<Integer,List<Bucket>> bucMap = sampling.getGroupBuckets();
		sampling.materialize();
//		int totalTuple = 0;
//		int totalSample = 0;
//		for(Entry<Integer, List<Bucket>> entry : bucMap.entrySet()){
//			List<Bucket> bucs = entry.getValue();
//			int sampleSize = 0;
//			int tupleSize = 0;
//			for(Bucket b : bucs){
//				sampleSize += b.getSampleNum();
//				tupleSize += b.getTupleNum();
//			}
//			totalSample += sampleSize;
//			totalTuple += tupleSize;
//			System.out.println(entry.getKey() + "--tupleNum:" + tupleSize + " , sampleNum:" + sampleSize + " , bucketNum:" + bucs.size());
//		}
//		
//		System.out.println("total tuple : " + totalTuple);
//		System.out.println("total sample : " + totalSample);
//		System.out.println("sampling rate : "  + (float)totalSample/totalTuple);
		
	}
	@Test
	public void testErrorBoundedSamplingWithJoin(){
		
		List<String> groupingAttrs = new ArrayList<String>();
		groupingAttrs.add("time_by_day.month_of_year");
		
		List<String> aggregateAttrs = new ArrayList<String>();
		aggregateAttrs.add("store_invoice");
		
		Map<String,String> mappingJoinAttrs = new HashMap<String,String>();
		mappingJoinAttrs.put(this.originTable+".time_id", "time_by_day.time_id");
		
		float error = 5;
		float confidence = 0.9f;
		
		ErrorBoundedSampling sampling = new ErrorBoundedSampling(error, confidence, originTable, originTable+"_error_bounded_join", groupingAttrs,mappingJoinAttrs, aggregateAttrs);
		Map<Integer,List<Bucket>> bucMap = sampling.getGroupBuckets();
		sampling.materialize();
		int totalTuple = 0;
		int totalSample = 0;
		for(Entry<Integer, List<Bucket>> entry : bucMap.entrySet()){
			List<Bucket> bucs = entry.getValue();
			int sampleSize = 0;
			int tupleSize = 0;
			for(Bucket b : bucs){
				sampleSize += b.getSampleNum();
				tupleSize += b.getTupleNum();
			}
			totalSample += sampleSize;
			totalTuple += tupleSize;
			System.out.println(entry.getKey() + "--tupleNum:" + tupleSize + " , sampleNum:" + sampleSize + " , bucketNum:" + bucs.size());
		}
		
		System.out.println("total tuple : " + totalTuple);
		System.out.println("total sample : " + totalSample);
		System.out.println("sampling rate : "  + (float)totalSample/totalTuple);
		
	}
	@Test
	public void testAvgError(){
		String originSql = "SELECT avg(store_invoice) FROM "+this.originTable + " group by store_id";
		String sampleSql = "SELECT avg(store_invoice) FROM "+this.sampleTable+ " group by store_id";
		ResultSet originResult = new DBUtil().excuteQuery(originSql);
		ResultSet sampleResult = new DBUtil().excuteQuery(sampleSql);
		
		try {
			int count = 0;
			while(originResult.next()){
				
				sampleResult.next();
				float exactValue = originResult.getFloat(1);
				float approxValue = sampleResult.getFloat(1);
				System.out.println("error "+ (count++) + ":"+Math.abs(approxValue-exactValue)/exactValue);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testCountError(){
		String originSql = "SELECT count(*) FROM "+this.originTable + " group by store_id";
		String sampleSql = "SELECT sum(sf) FROM "+this.sampleTable+ " group by store_id";
		ResultSet originResult = new DBUtil().excuteQuery(originSql);
		ResultSet sampleResult = new DBUtil().excuteQuery(sampleSql);
		
		try {
			int count = 0;
			while(originResult.next()){
				
				sampleResult.next();
				float exactValue = originResult.getFloat(1);
				float approxValue = sampleResult.getFloat(1);
				System.out.println("error "+ (count++) + ":"+Math.abs(approxValue-exactValue)/exactValue);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testSumError(){
		String originSql = "SELECT sum(store_invoice) FROM "+this.originTable + " group by store_id";
		String sampleSql = "SELECT sum(store_invoice)*sf FROM "+this.sampleTable+ " group by store_id";
		ResultSet originResult = new DBUtil().excuteQuery(originSql);
		ResultSet sampleResult = new DBUtil().excuteQuery(sampleSql);
		
		try {
			int count = 0;
			while(originResult.next()){
				
				sampleResult.next();
				float exactValue = originResult.getFloat(1);
				float approxValue = sampleResult.getFloat(1);
				System.out.println("error "+ (count++) + ":"+Math.abs(approxValue-exactValue)/exactValue);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTypeName(){
		ResultSet rs = new DBUtil().excuteQuery("SELECT * FROM  time_by_day");
		ResultSetMetaData meta = null;
		try {
			meta = rs.getMetaData();
			int colNum = meta.getColumnCount();
			for(int i = 0 ; i < colNum; i++){
				System.out.println(meta.getColumnTypeName(i+1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
