package mondrian.sampling;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import db.DBUtil;

/**
 * Congressional Sampling：
 * 1、基于单个表进行抽样
 * 输入：
 * 1)需要进行抽样的原始表
 * 2)分组属性集
 * 3)生成的样本表名
 * 4)抽样率
 * 
 * TODO 描述如下:
 * 2、基于多个表的进行抽样(事实表+维表)
 * 输入：
 * 1)需要进行抽样的表、抽样需要进行连接的表
 * 2)分组属性集(来源于抽样表以及连接表)
 * 3)生成的样本表名
 * 4)多表连接时连接属性的映射关系
 * 5)抽样率
 * 
 * TODO 浮点运算的精确度问题、固定样本大小的抽取
 * @Description Congressional Sampling 实现
 * @author zoe
 * @date 2017年4月6日 上午11:47:04 
 *
 */
public class CongressionalSampling {
	
	//与操作的表相关的信息
	private String originTable ;								//required
	private String sampleTable ;								//required
	private List<String> joinTables;				//optional 与抽样表进行连接的表以及要进行group-by操作的属性
	private Map<String, String> mappingJoinAttrs;				//optional 抽样表与连接表的连接属性映射
	private List<String> groupingAttrs;
	private List<String> groupingAttrsType;
	private float samplingRate;
	
	//与抽样相关的成员
	private int groupNum;										//基于分组属性的分组数目
	private Map<Integer,List<String>> groupingValues;			//基于分组属性的所有取值
 	private List<Integer> tupleNumOfGrouping;					//基于分组属性的各个分组下的总元组数
	private int sampleNum;										//样本空间：数据总量*样本率
	private List<Float> house;									//house样本分配
	private List<Float> senate;									//senate样本分配
	private List<Float>	basicCongress;							//basic congress样本分配
	private List<Float> congress;								//congress样本分配
	private List<List<Integer>>	subGroupings;					//所有基于分组属性子集的分组情形
	
	/**
	 * Congressional 基于单表的抽样方案构造器 。
	 * 参数 groupingAttr格式在不同的抽样情形下的差异：
	 * 1)基于单表的抽样方案：[attr1, attr2, ..., attrn]
	 * 2)基于多表的抽样方案：[table.attr,.....]
	 * 
	 * @param originTable	原始表名(抽样表)
	 * @param sampleTable	样本表名
	 * @param groupingAttrs	分组属性集
	 * @param samplingRate 抽样率
	 */
	public CongressionalSampling(String originTable, String sampleTable,  ArrayList<String> groupingAttrs, float samplingRate){
		
		//获取相关表信息
		this.originTable = originTable;
		this.sampleTable = sampleTable;
		this.samplingRate = samplingRate;
		this.groupingAttrs = groupingAttrs;
		this.groupingAttrsType = new ArrayList<String>();
		
		//根据表信息初始化与抽样相关的成员变量
		this.groupingValues = new HashMap<Integer, List<String>>();
		this.tupleNumOfGrouping = new ArrayList<Integer>();
		this.house = new ArrayList<Float>();
		this.senate = new ArrayList<Float>();
		this.congress = new ArrayList<Float>();
		this.basicCongress = new ArrayList<Float>();
		this.subGroupings = new ArrayList<List<Integer>>();
		init();
	}
	/**
	 * Congressional 基于多表的抽样方案构造器 。
	 * 参数 groupingAttr格式在不同的抽样情形下的差异：
	 * 1)基于单表的抽样方案：[attr1, attr2, ..., attrn]
	 * 2)基于多表的抽样方案：[table.attr,.....]
	 * 
	 * @param originTable	原始表名(抽样表)
	 * @param sampleTable	样本表名
	 * @param groupingAttrs	分组属性集
	 * @param mappingJoinAttrs 抽样表与连接表的连接属性映射关系,如果为null,表示为基于单表的抽样
	 * @param samplingRate 抽样率
	 */
	public CongressionalSampling(String originTable, String sampleTable,  ArrayList<String> groupingAttrs,Map<String, String> mappingJoinAttrs, float samplingRate){
		
		//获取相关表信息
		this.originTable = originTable;
		this.sampleTable = sampleTable;
		this.samplingRate = samplingRate;
		this.groupingAttrs = groupingAttrs;
		this.groupingAttrsType = new ArrayList<String>();
		
		this.mappingJoinAttrs = mappingJoinAttrs;
		
		//检查分组属性与连接属性来源表是否一致
		checkMapping(groupingAttrs, mappingJoinAttrs);
		//根据表信息初始化与抽样相关的成员变量
		this.groupingValues = new HashMap<Integer, List<String>>();
		this.tupleNumOfGrouping = new ArrayList<Integer>();
		this.house = new ArrayList<Float>();
		this.senate = new ArrayList<Float>();
		this.congress = new ArrayList<Float>();
		this.basicCongress = new ArrayList<Float>();
		this.subGroupings = new ArrayList<List<Integer>>();
		initWithJoin();
	}
	
	/**
	 * 检查分组属性的来源表与连接属性映射关系的来源表是否一致
	 * 
	 * @param groupingAttrs	
	 * @param mappingJoinAttrs
	 */
	private void checkMapping(ArrayList<String> groupingAttrs,
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
		this.joinTables = new ArrayList<String>();
		this.joinTables.addAll(allTables);
	}
	
	public CongressionalSampling(){
		//读取配置文件，获取相关表信息
		Properties prop = new Properties();
		try {
			prop.load(CongressionalSampling.class.getResourceAsStream("/TableInfo.properties"));
			this.originTable = prop.getProperty("table.name");
			this.samplingRate = Float.valueOf(prop.getProperty("table.ratio"));
			this.groupingAttrs = Arrays.asList(prop.getProperty("table.groupingAttrs").split(","));
			this.groupingAttrsType = new ArrayList<String>();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//根据表信息初始化与抽样相关的成员变量
		this.groupingValues = new HashMap<Integer, List<String>>();
		this.tupleNumOfGrouping = new ArrayList<Integer>();
		this.house = new ArrayList<Float>();
		this.senate = new ArrayList<Float>();
		this.congress = new ArrayList<Float>();
		this.basicCongress = new ArrayList<Float>();
		this.subGroupings = new ArrayList<List<Integer>>();
		init();
	}
	/**
	 * 基于多表进行抽样的初始化方法，需要进行多个表的连接操作
	 * 
	 * 1、查询数据库，基于分组属性进行分组
	 * 2、要填充的变量：分组属性名、分组属性类型、所有分组取值、分组数目、分组属性类型（要根据类型来进行值的拼接）
	 */
	private void initWithJoin(){
		StringBuilder attrSet=  new StringBuilder();
		StringBuilder fromTables = new StringBuilder();
		StringBuilder whereClause = new StringBuilder();
		for(String attr : this.groupingAttrs){
			attrSet.append(attr +",");
		}
		for(String table : this.joinTables){
			fromTables.append(table + ",");
		}
		for(Entry<String,String> entry : this.mappingJoinAttrs.entrySet()){
			whereClause.append(entry.getKey()+"="+entry.getValue() + " AND ");
		}
		StringBuilder groupingSql = new StringBuilder();
		groupingSql.append("SELECT "+ attrSet +" count(*) ");
		groupingSql.append(" FROM "+ fromTables.toString() + this.originTable);
		groupingSql.append(" WHERE " + whereClause.substring(0,whereClause.lastIndexOf("AND")));
		groupingSql.append(" GROUP BY " +attrSet.substring(0,attrSet.length()-1));
		
		System.out.println("grouping sql:" + groupingSql);
		
		DBUtil util = new DBUtil();
		ResultSet rs = util.excuteQuery(groupingSql.toString());
		ResultSetMetaData meta = null;
		
		try {
			meta = rs.getMetaData();
			int rowNum = 0;
			List<String> values = null;
			int totalTupleNum = 0;
			
			/**
			 * populate grouping attrs' type
			 */
			for(int col = 1; col <= this.groupingAttrs.size(); col++ ){
				this.groupingAttrsType.add(meta.getColumnTypeName(col));
			}
			
			while(rs.next()){
				values = new ArrayList<String>();
				for(int col = 1; col <= this.groupingAttrs.size(); col++ ){
					values.add(rs.getString(col));
				}
				//为每种分组情况设置对应的值，以及各个分组的数目
				this.groupingValues.put(rowNum++, values);
				int tupleNum = rs.getInt(this.groupingAttrs.size()+1);
				totalTupleNum += tupleNum;
				this.tupleNumOfGrouping.add(tupleNum);
			}
			//设置分组大小、样本空间
			this.groupNum = this.groupingValues.size();
			this.sampleNum = (int) (totalTupleNum * this.samplingRate);
			System.out.println("total tuples:" + this.tupleNumOfGrouping);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 基于单表进行抽样的初始化方法
	 * 
	 * 1、查询数据库，基于分组属性进行分组
	 * 2、要填充的变量：分组属性名、分组属性类型、所有分组取值、分组数目、分组属性类型（要根据类型来进行值的拼接）
	 */
	private void init(){
		StringBuilder attrSet=  new StringBuilder();
		for(String attr : this.groupingAttrs){
			attrSet.append(attr +",");
		}
		String groupingSql = "SELECT "+ attrSet +" count(*) FROM "+ this.originTable + " GROUP BY "+attrSet.substring(0,attrSet.length()-1);
		System.out.println("grouping sql:" + groupingSql);
		DBUtil util = new DBUtil();
		ResultSet rs = util.excuteQuery(groupingSql);
		ResultSetMetaData meta = null;
		try {
			meta = rs.getMetaData();
			int rowNum = 0;
			List<String> values = null;
			int totalTupleNum = 0;
			
			/**
			 * populate grouping attrs' type
			 */
			for(int col = 1; col <= this.groupingAttrs.size(); col++ ){
				this.groupingAttrsType.add(meta.getColumnTypeName(col));
			}
			
			while(rs.next()){
				values = new ArrayList<String>();
				for(int col = 1; col <= this.groupingAttrs.size(); col++ ){
					values.add(rs.getString(col));
				}
				//为每种分组情况设置对应的值，以及各个分组的数目
				this.groupingValues.put(rowNum++, values);
				int tupleNum = rs.getInt(this.groupingAttrs.size()+1);
				totalTupleNum += tupleNum;
				this.tupleNumOfGrouping.add(tupleNum);
			}
			//设置分组大小、样本空间
			this.groupNum = this.groupingValues.size();
			this.sampleNum = (int) (totalTupleNum * this.samplingRate);
			System.out.println("total tuples:" + this.tupleNumOfGrouping);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 对每个组，按比例分配样本空间
	 */
	public void house(){
		
		for(int num : this.tupleNumOfGrouping){
			// TODO 或者用另外的计算方法：样本大小*(某分组元组数/总元组数)
			this.house.add(num*this.samplingRate);
		}
		
		System.out.println("house : "+this.house);
	}
	/**
	 * 对每个组，平均分配样本空间
	 */
	public void senate(){
		this.senate.add((float)this.sampleNum/this.groupNum);
		
		System.out.println("senate : "+this.senate);
	}
	/**
	 * 对每个组，取max(house,senate)
	 * 按比例减小，将样本空间控制在指定大小
	 */
	public void basicCongress(){
		float senate = this.senate.get(0);
		float maxSum = 0;
		for(float sampleSize : this.house){
			maxSum += Math.max(sampleSize, senate);
		}
		
		for(float houseSample : this.house){
			this.basicCongress.add(this.sampleNum*Math.max(houseSample, senate)/maxSum);
		}
		
		System.out.println("basic congress :"+this.basicCongress);
	}
	/**
	 * 考虑对分组属性上的子集T进行分组的情形
	 * 基于子集进行分组，对每个组，样本空间平均分配
	 * 在父分组内，计算子分组占父分组的比例大小，用 父分组大小*（子分组大小/父分组大小）
	 * 将每种子集分组情形下，记录子分组的最大分配
	 * 最后统一按比例减小到指定的样本空间大小
	 */
	public void congress(){
		//初始化congress样本分配，将值置为house、senate、basic congress中的最大值
		for(int i = 0; i < this.groupNum; i++){
			this.congress.add(Math.max(
									Math.max(this.house.get(i), this.senate.get(0)),
									this.basicCongress.get(i)
									)
							);
		}
		//计算基于分组属性集上的子集的所有分组情形
		computeSubGroupings();
		
		//对每一种分组情形，构造数据库查询，计算空间分配
		for(int i = 0; i< this.subGroupings.size(); i++){
			List<Integer> subGrouping = this.subGroupings.get(i);
			StringBuilder attrs = new StringBuilder();
			String sql = null;
			Set<String> fromTables = new HashSet<String>();
			for(int attrIndex : subGrouping){
				String attr = this.groupingAttrs.get(attrIndex);
				//当为多表连接抽样时，收集当前分组情形下需要连接的表
				if(!(this.mappingJoinAttrs == null || this.mappingJoinAttrs.isEmpty()))
					fromTables.add(attr.split("\\.")[0]);
				attrs.append(attr + ",");
			}
			fromTables.remove(this.originTable);
			StringBuilder fromClause = new StringBuilder("");
			for(String table : fromTables){
				fromClause.append(table+",");
			}
			sql = "SELECT " + attrs + " count(*) FROM " + fromClause.toString() +this.originTable + " GROUP BY "+ attrs.substring(0,attrs.length()-1);
			System.out.println("sub group sql:"+sql);
			DBUtil util = new DBUtil();
			ResultSet rs = util.excuteQuery(sql);
			Map<Integer, Float> ratio = new HashMap<Integer, Float>();
			try {
				int parentTupleNum = 0;
				while(rs.next()){
					int parentNum = rs.getInt(subGrouping.size()+1);
					
					//遍历每一个分组属性值，判断是否为当前父分组的子分组 
					for(Entry<Integer, List<String>> entry : this.groupingValues.entrySet()){
						boolean equalFlag = true;
						
						for(int attrNum = 0; attrNum < subGrouping.size(); attrNum++){
							int valueIndex = subGrouping.get(attrNum);
							if(!entry.getValue().get(valueIndex).equals(rs.getString(attrNum+1))){
								equalFlag = false;
								break;
							}
						}
						//如果当前分组是结果集父分组的子分组，计算子分组占父分组的比例
						if(equalFlag){
							ratio.put(entry.getKey(),(float)this.tupleNumOfGrouping.get(entry.getKey())/parentNum);
						}
					}
					parentTupleNum++;
				}
				//计算父分组在senate方法下分配到的样本空间
				int parentSpace = this.sampleNum/parentTupleNum;
				//父分组结果集遍历完毕，遍历具有比例信息的集合，在congress中保持当前最大值
				for(Entry<Integer, Float> entry : ratio.entrySet()){
					float temp =  parentSpace*entry.getValue();
					if(this.congress.get(entry.getKey()) < temp)
						this.congress.set(entry.getKey(),temp);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}// outside for
		
		//所有分组情形已经遍历完毕，现在按比例进行缩减，并计算对应的比例因子
		int maxSum = 0;
		for(float sizeBeforeScale : this.congress){
			maxSum += sizeBeforeScale;
		}
		for(int j = 0 ;j< this.congress.size();j++){
			float sizeAfterScale = this.congress.get(j)/maxSum*this.sampleNum;
			this.congress.set(j, sizeAfterScale);
		}
		System.out.println("congress:"+this.congress);
	}
	/**
	 * 计算分组属性集合子集的所有分组情形，除了空集和全集
	 */
	@Test
	public void computeSubGroupings(){
		//循环计算每一种分组属性上的子集(不包含空集、全集)
		for(int subNum = 1; subNum < this.groupingAttrs.size(); subNum++){
			
			int pos = 0;					//当前起始属性位置
			int remainNum = subNum-1;		//除去当前位置的属性，余下还需要取的属性数目
			List<Integer> currentSubGrouping = null;
			if(remainNum != 0){
				while((pos+subNum) <= this.groupingAttrs.size()){
					for(int i = pos+1; (i+remainNum) <= this.groupingAttrs.size(); i++){
						//对当前位置属性，每次向后移动一个位置，连续取remainNum个属性值
						currentSubGrouping = new ArrayList<Integer>();
						currentSubGrouping.add(pos);
						for(int j = 0; j<remainNum; j++){
							currentSubGrouping.add(i+j);
						}
						this.subGroupings.add(currentSubGrouping);
					}//for
					//当前位置向后移动一个单位
					pos++;
				}//while
			}else{
				for(int j = 0; j<this.groupingAttrs.size(); j++){
					currentSubGrouping = new ArrayList<Integer>();
					currentSubGrouping.add(j);
					this.subGroupings.add(currentSubGrouping);
				}
			}
		}
	}
	/**
	 * 物化基于单表的congress抽样技术得到的样本。
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：(rowID, 原始属性集, scale factor)
	 */
	public void materializeCongress(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.sampleTable + " SELECT * FROM " + this.originTable +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.sampleTable +" ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.sampleTable +" ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  " SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.congress.get(i) + " AS sf"
					+ " FROM "+ this.originTable + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.congress.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeCongress : "+sql);
			
			new DBUtil().excute(sql);
			
		}
	}
	/**
	 * 物化基于多表的congress抽样技术得到的样本。
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：(rowID, 原始属性集, scale factor)
	 */
	public void materializeCongressWithJoin(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		StringBuilder fromTables = null;
		StringBuilder whereClause = null;
		
		new DBUtil().excute("CREATE TABLE " + this.sampleTable + " SELECT * FROM " + this.originTable +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.sampleTable +" ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			fromTables = new StringBuilder();
			whereClause = new StringBuilder();
			
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.sampleTable +" ";
			
			for(String table : this.joinTables){
				fromTables.append(table + ",");
			}
			for(Entry<String,String> entry : this.mappingJoinAttrs.entrySet()){
				whereClause.append(entry.getKey()+"="+entry.getValue() + " AND ");
			}
			
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  " SELECT "+this.originTable+".*, "+ this.tupleNumOfGrouping.get(i)/this.congress.get(i) + " AS sf"
					+ " FROM "+ fromTables.toString()+ this.originTable 
					+ " WHERE " + whereClause.toString() + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.congress.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeCongress : "+sql);
			
			new DBUtil().excute(sql);
			
		}
	}
	/**
	 * 物化基于senate抽样技术得到的样本。(just for test)
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_senate_sample(原始属性集, sf)
	 */
	public void materializeSenate(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.originTable +"_senate_sample " + " SELECT * FROM " + this.originTable +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.originTable +"_senate_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.originTable +"_senate_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.senate.get(0) + " AS sf"
					+ " FROM "+ this.originTable + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.senate.get(0)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeSenate : "+sql);
			new DBUtil().excute(sql);
		}
	}
	/**
	 * 物化基于house抽样技术得到的样本。(just for test)
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_house_sample(原始属性集, sf)
	 */
	public void materializeHouse(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.originTable +"_house_sample " + " SELECT * FROM " + this.originTable +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.originTable +"_house_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.originTable +"_house_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.house.get(i) + " AS sf"
					+ " FROM "+ this.originTable + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.house.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeHouse : "+sql);
			new DBUtil().excute(sql);
			
		}
	}
	
	/**
	 * 物化基于basic congress抽样技术得到的样本。(just for test)
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_basic_congress_sample(原始属性集, sf)
	 */
	public void materializeBasicCongress(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.originTable +"_basic_congress_sample " + " SELECT * FROM " + this.originTable +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.originTable +"_basic_congress_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.originTable +"_basic_congress_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.basicCongress.get(i) + " AS sf"
					+ " FROM "+ this.originTable + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.basicCongress.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeBasicCongress : "+sql);
			new DBUtil().excute(sql);
		}
		//将当前样本信息写入样本元数据中
		write2Meta();
	}
	
	private void write2Meta(){
		SampleInfo info = new SampleInfo();
		boolean isMulti = this.mappingJoinAttrs == null || this.mappingJoinAttrs.isEmpty()?
				false:true;
		List<String> joinAttrs = new ArrayList<String>();
		for(Entry<String, String> entry : this.mappingJoinAttrs.entrySet()){
			joinAttrs.add(entry.getKey()+":"+entry.getValue());
		}
		info.setGroupingAttrs(this.groupingAttrs);
		info.setJoinAttrs(joinAttrs);
		info.setMultiTableSampling(isMulti);
		info.setOriginTable(this.originTable);
		info.setSampleTable(this.sampleTable);
		info.setSamplingRate(this.samplingRate);
		info.setSamplingSchema(SamplingSchema.CONGRESSIONAL_SAMPLING);
		SampleInfoWriter.write(info);
	}
}
