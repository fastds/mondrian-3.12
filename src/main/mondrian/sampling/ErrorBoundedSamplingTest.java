package mondrian.sampling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

/**
 * 
 * @Description 对Error-bounded抽样方法的测试
 * @author zoe
 * @date 2017年4月11日 下午6:27:26 
 *
 */
public class ErrorBoundedSamplingTest {
	@Test
	public void testErrorBoundedSampling(){
		String table = "inventory_fact_1997";
		List<String> groupingAttrs = new ArrayList<String>();
		groupingAttrs.add("store_id");
		
		List<String> aggregateAttrs = new ArrayList<String>();
		aggregateAttrs.add("store_invoice");
		
		float error = 5;
		float confidence = 0.9f;
		
		ErrorBoundedSampling sampling = new ErrorBoundedSampling(error, confidence, table, groupingAttrs, aggregateAttrs);
		Map<Integer,List<Bucket>> bucMap = sampling.getGroupBuckets();
		
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
		
		System.out.println("total tuple:"+totalTuple);
		System.out.println("total sample:"+totalSample);
		System.out.println("sampling rate:"+(float)totalSample/totalTuple);
			
	}
}
