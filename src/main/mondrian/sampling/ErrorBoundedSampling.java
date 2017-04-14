package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import db.DBUtil;

/**
 * 一、基于单表的抽样
 * 根据用户指定的误差和置信度，计算需要进行采样大小。计算过程如下：
 * 1、基于用户指定的分组属性查询数据库，查询用户所指定的聚集列的值
 * 2、计算每个分组的buckets，计算各个bucket的采样率
 * 3、根据所有bucket和对应的采样率，对原始表的进行随机抽样
 * 4、物化样本
 * 组g上的误差的定义：
 * 误差 = |估计量均值-统计量均值|
 * 
 * 二、基于多表的抽样需求
 * 过程与一一致，只是考虑分组属性来源于维表的情况，需要进行连接操作，再针对连接结果进行应用抽样schema
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
	private String originTable;							//原始表
	private String sampleTable;							//样本表
	private List<String> groupingAttrs;					//分组属性
	private List<String> aggregateAttrs;				//聚集属性,虽然是个集合，但里面只有一个元素，也默认只处理一个元素（第一个）
	private List<String> joinTables;					//optional 与抽样表进行连接的表以及要进行group-by操作的属性
	/*
	 * 通过属性mappingJoinAttrs是否为空或者empty来判断是否为基于多表的抽样
	 * optional 抽样表与连接表的连接属性映射
	 */
	private Map<String, String> mappingJoinAttrs;		
	
	/**
	 * 以下为需要从数据库查询并加载的属性
	 */
	private Map<Integer,List<String>> groupingValues;	//每个分组对应的分组属性的取值
	private List<String> groupingAttrsType;				//分组属性对应的类型
	private Map<Integer,List<Bucket>> groupBuckets;		//每个分组对应的bucket的集合
 	private List<Double> scaleFactors;					//每个分组的比例因子
 	private float samplingRate;							//抽样率
 	
 	/**
 	 * 基于单表进行抽样的构造器
 	 * 
 	 * @param error				误差
 	 * @param confidence		置信度
 	 * @param originTable		原始表
 	 * @param sampleTable		样本表
 	 * @param groupingAttrs		分组属性集={attr1, attr2, attr3, ...}
 	 * @param aggregateAttrs	聚集属性集，虽然定义为集合，但只包含一个元素，总是获取第一个
 	 */
	public ErrorBoundedSampling(float error, float confidence
			,String originTable,String sampleTable, List<String> groupingAttrs, List<String> aggregateAttrs){
		this.error = error;
		this.confidence = confidence;
		this.originTable = originTable;
		this.sampleTable = sampleTable;
		this.groupingAttrs = groupingAttrs;
		this.aggregateAttrs = aggregateAttrs;
		
		this.groupingValues = new HashMap<Integer, List<String>>();
		this.groupingAttrsType = new ArrayList<String>();
		this.groupBuckets = new HashMap<Integer, List<Bucket>>();
		this.scaleFactors = new ArrayList<Double>();
		
		//其他属性初始化
		this.mappingJoinAttrs = new HashMap<String, String>();
		this.joinTables = new ArrayList<String>();
		load();
	}
	/**
	 * 基于多表进行抽样的构造器
	 * 
	 * @param error				误差
	 * @param confidence		置信度
	 * @param originTable		原始表
	 * @param sampleTable		样本表
	 * @param groupingAttrs		分组属性={table.attr...}
	 * @param mappingJoinAttrs	原始表以及与其进行连接的表的连接属性映射关系
	 * 							<originTable.joinAttr1, otherTable.joinAttr1>
	 * @param aggregateAttrs	聚集属性
	 */
	public ErrorBoundedSampling(float error, float confidence
			,String originTable,String sampleTable, List<String> groupingAttrs,
			 Map<String, String> mappingJoinAttrs, List<String> aggregateAttrs){
		
		//设置用户参数
		this.error = error;
		this.confidence = confidence;
		this.originTable = originTable;
		this.sampleTable = sampleTable;
		this.groupingAttrs = groupingAttrs;
		this.aggregateAttrs = aggregateAttrs;
		this.mappingJoinAttrs = mappingJoinAttrs;
		
		//待填充属性初始化
		this.joinTables = new ArrayList<String>();
		//检查分组属性与连接属性来源表是否一致
		checkMapping(groupingAttrs, mappingJoinAttrs);
		
		this.groupingValues = new HashMap<Integer, List<String>>();
		this.groupingAttrsType = new ArrayList<String>();
		this.groupBuckets = new HashMap<Integer, List<Bucket>>();
		this.scaleFactors = new ArrayList<Double>();
		loadWithJoin();
	}
	/**
	 * 检查分组属性的来源表与连接属性映射关系的来源表是否一致
	 * 并初始化属性joinTables
	 * 
	 * @param groupingAttrs		分组属性
	 * @param mappingJoinAttrs	join属性映射关系，left=originTable.joinAttr1...
	 */
	private void checkMapping(List<String> groupingAttrs,
			Map<String, String> mappingJoinAttrs) {
		Set<String> allTables = new HashSet<String>();
		
		//收集分组属性中的所有表
		for(String groupingAttr : groupingAttrs){
			allTables.add(groupingAttr.split("\\.")[0]);
		}
		
		allTables.remove(this.originTable);
		
		Set<String> allTables2 = new HashSet<String>();
		/*
		 * 检查连接属性映射关系中的抽样表是否唯一,收集连接属性映射关系中的所有表
		 * Map<String,String> = <table1.joinAttr,table2.joinAttr>
		 * table1==this.originTable
		 */
		for(Entry<String,String> entry : mappingJoinAttrs.entrySet()){
			allTables2.add(entry.getKey().split("\\.")[0]);
			allTables2.add(entry.getValue().split("\\.")[0]);
		}
		
		//仅考虑抽样的原始表之外的表
		allTables2.remove(this.originTable);
		
		//检查分组属性与连接属性二者的来源表是否一致
		if(allTables.size() != allTables2.size())
			//TODO 抛出参数不一致异常
			throw new RuntimeException("参数不一致！");
		for(String table : allTables){
			allTables2.remove(table);
		}
		if(allTables2.size() != 0)
			//TODO 抛出参数不一致异常
			throw new RuntimeException("参数不一致");
		
		//通过检查，填充查询涉及的表
		this.joinTables.addAll(allTables);
	}

	/**
	 * 数据加载
	 * 从table中加载属性，将查询结果按升序排序
	 * 获取所有分组，初始化groupBuckets
	 * 根据查询结果计算所有buckets
	 * 
	 */
	private void loadWithJoin(){
		StringBuilder grpAttrsStr=  new StringBuilder();
		StringBuilder fromTables = new StringBuilder();
		StringBuilder whereClause = new StringBuilder();
		for(String attr : this.groupingAttrs){
			grpAttrsStr.append(attr +",");
		}
		for(String table : this.joinTables){
			fromTables.append(table + ",");
		}
		for(Entry<String,String> entry : this.mappingJoinAttrs.entrySet()){
			whereClause.append(entry.getKey()+"="+entry.getValue() + " AND ");
		}
		
		StringBuilder loadSql = new StringBuilder();
		
		//计算有多少个组，设置每个组对应的分组属性的取值
		loadSql.append(" SELECT " + grpAttrsStr.substring(0,grpAttrsStr.lastIndexOf(",")));
		loadSql.append(" FROM "+ fromTables + this.originTable);
		loadSql.append(" WHERE "+ whereClause.substring(0,whereClause.lastIndexOf("AND")));
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
			
			/**
			 *	计算scale factor 以及抽样率
			 */
			int totalTupleNum = 0;
			int totalSampleNum = 0;
			for(int i = 0;i < this.groupBuckets.size(); i++){
				List<Bucket> bucs = this.getGroupBuckets().get(i);
				int tupleNum = 0;
				int sampleNum = bucs.size();
				for(Bucket buc : bucs){
					tupleNum += buc.getTupleNum();
				}
				totalTupleNum += tupleNum;
				totalSampleNum += sampleNum;
				
				this.scaleFactors.add((double)tupleNum/sampleNum);
			}
			
			this.samplingRate = (float)totalSampleNum/totalTupleNum;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 数据加载
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
		loadSql.append(" FROM "+ this.originTable);
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
			
			/**
			 *	计算scale factor 以及抽样率
			 */
			int totalTupleNum = 0;
			int totalSampleNum = 0;
			for(int i = 0;i < this.groupBuckets.size(); i++){
				List<Bucket> bucs = this.getGroupBuckets().get(i);
				int tupleNum = 0;
				int sampleNum = bucs.size();
				for(Bucket buc : bucs){
					tupleNum += buc.getTupleNum();
				}
				totalTupleNum += tupleNum;
				totalSampleNum += sampleNum;
				
				this.scaleFactors.add((double)tupleNum/sampleNum);
			}
			
			this.samplingRate = (float)totalSampleNum/totalTupleNum;
			
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
		StringBuilder fromTables = new StringBuilder();
		StringBuilder joinWhere = new StringBuilder();
		for(String table : this.joinTables){
			fromTables.append(table + ",");
		}
		for(Entry<String,String> entry : this.mappingJoinAttrs.entrySet()){
			joinWhere.append(entry.getKey()+"="+entry.getValue() + " AND ");
		}
		//遍历每个分组，为每个分组计算bucket集合
		for(int i = 0 ; i < this.groupingValues.size(); i++){
			aggAttrSetSql = new StringBuilder();
			aggAttrSetSql.append(" SELECT "+this.aggregateAttrs.get(0));
			//基于多表的FROM
			if(this.mappingJoinAttrs == null || this.mappingJoinAttrs.isEmpty())
				aggAttrSetSql.append(" FROM " + this.originTable);
			else//基于单表的FROM
				aggAttrSetSql.append(" FROM " + fromTables + this.originTable);
			aggAttrSetSql.append(" WHERE ");
			StringBuilder groupWhere = new StringBuilder();
			
			List<String> attrValues = this.groupingValues.get(i);
			
			for(int j = 0; j < attrValues.size(); j++){
				groupWhere.append(this.groupingAttrs.get(j) + "="+ attrValues.get(j)+" AND ");
			}
			//基于多表的join操作
			if(this.mappingJoinAttrs != null && !this.mappingJoinAttrs.isEmpty())
				aggAttrSetSql.append(joinWhere);
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
	/**
	 * 物化样本数据，针对每一个分组，计算每一个bukcet需要的rowid，将对应的数据插入样本表
	 * TODO 这个实现非常naive，以后需要进行优化。(存储过程？函数？)
	 */
	public void materialize(){
		StringBuilder materializeSql = null;
		StringBuilder createSql = new StringBuilder();
		createSql.append("CREATE TABLE " + this.sampleTable);
		createSql.append(" SELECT * ");
		createSql.append(" FROM " + this.originTable +" LIMIT 0 ");
		new DBUtil().excute(createSql.toString());
		new DBUtil().excute("ALTER TABLE " + this.sampleTable +" ADD COLUMN row_id int");
		new DBUtil().excute("ALTER TABLE " + this.sampleTable +" ADD COLUMN sf decimal(16,6)");
		//遍历每个分组，为每个分组计算bucket集合
		for(int i = 0 ; i < this.groupingValues.size(); i++){
			materializeSql = new StringBuilder();
			StringBuilder fromTables = new StringBuilder();
			StringBuilder joinWhere = new StringBuilder();
			for(String table : this.joinTables){
				fromTables.append(table + ",");
			}
			for(Entry<String,String> entry : this.mappingJoinAttrs.entrySet()){
				joinWhere.append(entry.getKey()+"="+entry.getValue() + " AND ");
			}
			materializeSql.append(" INSERT INTO " + this.sampleTable);
			materializeSql.append(" SELECT * FROM (");
			materializeSql.append(" SELECT "+this.originTable+".*, @row_id:= @row_id+1 AS row_id, " + this.scaleFactors.get(i) + " AS sf ");
			if(this.mappingJoinAttrs == null || this.mappingJoinAttrs.isEmpty())
				materializeSql.append(" FROM " + this.originTable + ", (SELECT @row_id:=0) as row_id");
			else //基于多表的FROM
				materializeSql.append(" FROM " + fromTables + this.originTable + ", (SELECT @row_id:=0) as row_id");
			materializeSql.append(" WHERE ");
			StringBuilder groupWhere = new StringBuilder();
			
			List<String> attrValues = this.groupingValues.get(i);
			
			for(int j = 0; j < attrValues.size(); j++){
				groupWhere.append(this.groupingAttrs.get(j) + "="+ attrValues.get(j)+" AND ");
			}
			StringBuilder inClause = new StringBuilder();
			//TODO 计算in中需要的值有哪些，然后拼接字符串
			int currentRow = 1;			//当前row_id，从1开始
			for(int j = 0 ;j <this.groupBuckets.get(i).size();j++){
				Bucket buc = this.groupBuckets.get(i).get(j);
				int tupleNum = buc.getTupleNum();
				int rowID = getRandInt(currentRow, currentRow+tupleNum);
				inClause.append(rowID+",");
				currentRow += tupleNum;
			}
			if(this.mappingJoinAttrs != null && !this.mappingJoinAttrs.isEmpty())
				materializeSql.append(joinWhere);
			materializeSql.append(groupWhere.substring(0, groupWhere.lastIndexOf("AND")));
			materializeSql.append(" ORDER BY " + this.aggregateAttrs.get(0)+" ASC ) AS tbl ");
			materializeSql.append(" WHERE row_id IN (" + inClause.substring(0,inClause.length()-1) + ")");
			
			
			System.out.println("materialize sql:"+materializeSql);
			
			new DBUtil().excute(materializeSql.toString());
			
		}// for end
	}

	private int getRandInt(int lo , int hi){
		Random rand = new Random();
			
		return lo + rand.nextInt(hi-lo+1);
	}
	
	/**
	 * 将样本信息存入数据库样本元数据中
	 */
	private void write2Meta(){
//		SampleInfo info = new SampleInfo();
//		boolean isMulti = this.mappingJoinAttrs == null || this.mappingJoinAttrs.isEmpty()?
//				false:true;
//		List<String> joinAttrs = new ArrayList<String>();
//		for(Entry<String, String> entry : this.mappingJoinAttrs.entrySet()){
//			joinAttrs.add(entry.getKey()+":"+entry.getValue());
//		}
//		info.setGroupingAttrs(this.groupingAttrs);
//		info.setJoinAttrs(joinAttrs);
//		info.setMultiTableSampling(isMulti);
//		info.setOriginTable(this.originTable);
//		info.setSampleTable(this.sampleTable);
//		info.setSamplingRate(this.samplingRate);
//		info.setSamplingSchema(SamplingSchema.CONGRESSIONAL_SAMPLING);
//		SampleInfoWriter.write(info);
	}

	public List<Double> getScaleFactors() {
		return scaleFactors;
	}

	public float getSamplingRate() {
		return samplingRate;
	}
	
}
