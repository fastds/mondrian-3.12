package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import db.DBUtil;

/**
 * 根据用户指定的误差和置信度，计算需要进行采样大小。计算过程如下：
 * 1、基于用户指定的分组属性查询数据库，查询用户所指定的聚集列的值
 * 2、计算每个分组的buckets，计算各个bucket的采样率
 * 3、根据所有bucket和对应的采样率，对原始表的进行随机抽样
 * 4、物化样本
 * 组g上的误差的定义：
 * 误差 = |估计量均值-统计量均值|
 * 
 * @Description Error-Bounded Sampling实现
 * @author zoe
 * @date 2017年4月6日 上午11:42:53 
 *
 */
public class ErrorBoundedSampling {
	
	/**
	 * 以下为用户指定的量
	 */
	private float error ;								//误差
	private float confidence;							//置信度
	private String table;								//原始表
	private List<String> groupingAttrs;					//分组属性
	private List<String> aggregateAttrs;				//聚集属性,虽然是个集合，但里面只有一个元素，也默认只处理一个元素（第一个）
	/**
	 * 以下为需要从数据库查询并加载的属性
	 */
	private Map<Integer,List<String>> groupingValues;	//每个分组对应的分组属性的取值
	private List<String> groupingAttrsType;				//分组属性对应的类型
	private Map<Integer,List<Bucket>> groupBuckets;		//每个分组对应的bucket的集合
 	
	public ErrorBoundedSampling(float error, float confidence
			,String table, List<String> groupingAttrs, List<String> aggregateAttrs){
		this.error = error;
		this.confidence = confidence;
		this.table = table;
		this.groupingAttrs = groupingAttrs;
		this.aggregateAttrs = aggregateAttrs;
		
		this.groupingValues = new HashMap<Integer, List<String>>();
		this.groupingAttrsType = new ArrayList<String>();
		this.groupBuckets = new HashMap<Integer, List<Bucket>>();
		
		load();
	}
	
	/**
	 * 从table中加载属性，将查询结果按升序排序
	 * 获取所有分组，初始化groupBuckets
	 * 根据查询结果计算所有buckets
	 * 
	 */
	private void load(){
		
		StringBuilder loadSql = new StringBuilder();
		StringBuilder grpAttrsStr = new StringBuilder();
		
		for(String temp:this.groupingAttrs){
			grpAttrsStr.append(temp+", ");
		}
		//计算有多少个组，设置每个组对应的分组属性的取值
		loadSql.append(" SELECT " + grpAttrsStr.substring(0,grpAttrsStr.lastIndexOf(",")));
		loadSql.append(" FROM "+ this.table);
		loadSql.append(" GROUP BY " + grpAttrsStr.substring(0,grpAttrsStr.lastIndexOf(",")));
		System.out.println("group by sql : "+loadSql.toString());
		
		DBUtil db = new DBUtil();
		ResultSet rs = null;
		rs = db.excuteQuery(loadSql.toString());
		ResultSetMetaData meta = null;
		try {
			int groupingNum = 0;
			meta = rs.getMetaData();
			/**
			 * 填充各个分组属性的类型
			 */
			for(int i = 0 ; i < this.groupingAttrs.size(); i++){
				String type = meta.getColumnTypeName(i+1);
				this.groupingAttrsType.add(type);
			}
			
			while(rs.next()){
				List<String> attrValues = new ArrayList<String>();
				/**
				 * 计算每个分组下，每个属性的取值
				 */
				for(int i = 0 ; i < this.groupingAttrs.size(); i++){
					
					String attrValue = rs.getString(i+1);
					attrValues.add(attrValue);
				}
				this.groupingValues.put(groupingNum++, attrValues);
			}
			
			/**
			 * 对每一个分组，计算其bucket集合
			 */
			calculateBuckets();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 针对每一个分组，从数据库查询聚集属性集，并按升序排序
	 * 并计算其bucket
	 */
	private void calculateBuckets(){
		StringBuilder aggAttrSetSql = null;
		
		//遍历每个分组，为每个分组计算bucket集合
		for(int i = 0 ; i < this.groupingValues.size(); i++){
			aggAttrSetSql = new StringBuilder();
			aggAttrSetSql.append(" SELECT "+this.aggregateAttrs.get(0));
			aggAttrSetSql.append(" FROM " + this.table);
			aggAttrSetSql.append(" WHERE ");
			StringBuilder groupWhere = new StringBuilder();
			
			List<String> attrValues = this.groupingValues.get(i);
			
			for(int j = 0; j < attrValues.size(); j++){
				groupWhere.append(this.groupingAttrs.get(j) + "="+ attrValues.get(j)+" AND ");
			}
			
			aggAttrSetSql.append(groupWhere.substring(0, groupWhere.lastIndexOf("AND")));
			aggAttrSetSql.append(" ORDER BY " + this.aggregateAttrs.get(0)+" ASC");
			
			System.out.println("grouping agg sql:"+aggAttrSetSql);
			
			ResultSet rs = new DBUtil().excuteQuery(aggAttrSetSql.toString());
			List<Number> sortedValue = new ArrayList<Number>();
			try {
				while(rs.next()){
					sortedValue.add(rs.getBigDecimal(1));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			List<Bucket> buckets = findBuckets(sortedValue);
			this.groupBuckets.put(i, buckets);
		}// for end
	}
	
	/**
	 * 为某一个组找出一个局部最佳buckets的划分
	 * 
	 * @param sortedValues 一个已排序的值{x1,...,xN}
	 * @return 该组值的桶的集合
	 */
	private List<Bucket> findBuckets(List<Number> sortedValues){
		
		List<Bucket> buckets = new ArrayList<Bucket>();
		int tupleNum = sortedValues.size();
		
		Bucket curr = new Bucket();
		curr.setTupleNum(1);
		curr.setSampleNum(1);
		curr.setLo(sortedValues.get(0).doubleValue());
		curr.setHi(sortedValues.get(0).doubleValue());
		buckets.add(curr);
		
		if(tupleNum == 1)
			return buckets;
		
		/*
		 * 从第二个元素开始计算，尝试将新值合并到当前桶中
		 * 如果合并操作增大了当前桶的值范围并且增加了误差下的样本量，则创建一个新的桶
		 */
		for(int i = 1 ; i < tupleNum; i++){
			double lo = curr.getLo();
			double hi = sortedValues.get(i).doubleValue();
			int sampleSize = calculateSampleSize(curr.getTupleNum(), lo, hi);
			/*
			 * 合并后的样本大小>之前的分配，划分到新的桶中
			 */
			if(sampleSize > curr.getSampleNum()){
				Bucket newBucket = new Bucket();
				newBucket.setTupleNum(1);
				newBucket.setSampleNum(1);
				newBucket.setLo(sortedValues.get(i).doubleValue());
				newBucket.setHi(sortedValues.get(i).doubleValue());
				buckets.add(newBucket);
				curr = newBucket;
			}else{	//否则，合并到当前桶中
				curr.setHi(hi);
				curr.setTupleNum(curr.getTupleNum()+1);
			}
		}
		return buckets;
	}
	/**
	 * 
	 * 计算当前桶的样本大小
	 * 
	 * @param tupleNum	当前桶中的元组数
	 * @param lo		当前桶的左边界
	 * @param hi		当前桶的右边界
	 * @return			当前桶的样本大小
	 */
	private int calculateSampleSize(int tupleNum,double lo, double hi){
		int sampleSize = 
				(int) Math.ceil(((hi-lo)*(hi-lo)*Math.log(2/(1-this.confidence)))/(2*this.error*this.error));
		return Math.min(tupleNum, sampleSize);
	}

	public Map<Integer, List<Bucket>> getGroupBuckets() {
		return groupBuckets;
	}


	
}
