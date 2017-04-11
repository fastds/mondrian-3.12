package mondrian.sampling;

import java.math.BigDecimal;

/**
 * 
 * @Description 用于error-bounded sampling 抽样方法的bucket
 * @author zoe
 * @date 2017年4月11日 下午3:48:59 
 *
 */
public class Bucket {
	private int tupleNum;		//桶中元组数
	private int sampleNum;		//桶中分配的样本量
	private double lo;		//当前桶的左边界
	private double hi;		//当前桶的右边界
	
	public int getTupleNum() {
		return tupleNum;
	}
	public void setTupleNum(int tupleNum) {
		this.tupleNum = tupleNum;
	}
	public int getSampleNum() {
		return sampleNum;
	}
	public void setSampleNum(int sampleNum) {
		this.sampleNum = sampleNum;
	}
	
	public double getLo() {
		return lo;
	}
	public void setLo(double lo) {
		this.lo = lo;
	}
	public double getHi() {
		return hi;
	}
	public void setHi(double hi) {
		this.hi = hi;
	}
	/**
	 * 比例因子 = 1/抽样率
	 * 
	 * @return 抽样率
	 */
	public float getSamplingRate(){
		return new Float(this.sampleNum)/this.tupleNum;
	}
	@Override
	public String toString() {
		return "Bucket [tupleNum=" + tupleNum + ", sampleNum=" + sampleNum + ", lo=" + lo + ", hi=" + hi + "]";
	}
	
	
}
