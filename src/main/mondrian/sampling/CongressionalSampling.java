package mondrian.sampling;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import java.util.Properties;

import db.DBUtil;

/**
 * 
 * @Description Congressional Sampling 实现
 * @author zoe
 * @date 2017年4月6日 上午11:47:04 
 *
 */
public class CongressionalSampling {
	//定义与操作的表相关的信息
	private String table ;
	private List<String> groupingAttrs;
	private float samplingRate;
	
	//定义与抽样相关的成员
	private int groupNum;										//基于分组属性的分组数目
	private Map<Integer,List<String>> groupingValues;			//基于分组属性的所有取值
	private List<Integer> tupleNumOfGrouping;					//基于分组属性的各个分组下的总元组数
	private int sampleNum;										//样本空间：数据总量*样本率
	private List<Float> house;									//house样本分配
	private List<Float> senate;									//senate样本分配
	private List<Float>	basicCongress;							//basic congress样本分配
	private List<Float> congress;								//congress样本分配
	private List<List<Integer>>	subGroupings;					//所有基于分组属性子集的分组情形
	public CongressionalSampling(){
		//读取配置文件，获取相关表信息
		Properties prop = new Properties();
		try {
			prop.load(CongressionalSampling.class.getResourceAsStream("/TableInfo.properties"));
			this.table = prop.getProperty("table.name");
			this.samplingRate = Float.valueOf(prop.getProperty("table.ratio"));
			this.groupingAttrs = Arrays.asList(prop.getProperty("table.groupingAttrs").split(","));
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
	 * 1、查询数据库，基于分组属性进行分组
	 * 2、要填充的变量：分组属性名、所有分组取值、分组数目
	 * 		
	 */
	private void init(){
		StringBuilder attrSet=  new StringBuilder();
		for(String attr : this.groupingAttrs){
			attrSet.append(attr +",");
		}
		String groupingSql = "SELECT "+ attrSet +" count(*) FROM "+ this.table + " GROUP BY "+attrSet.substring(0,attrSet.length()-1);
		System.out.println("grouping sql:" + groupingSql);
		DBUtil util = new DBUtil();
		ResultSet rs = util.excuteQuery(groupingSql);
		
		try {
			int rowNum = 0;
			List<String> values = null;
			int totalTupleNum = 0;
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
			for(int attrIndex : subGrouping){
				attrs.append(this.groupingAttrs.get(attrIndex) + ",");
			}
			sql = "SELECT " + attrs + " count(*) FROM " + this.table + " GROUP BY "+ attrs.substring(0,attrs.length()-1);
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
	 * 物化基于congress抽样技术得到的样本。
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_congress_sample(rowID, 原始属性集, scale factor)
	 */
	public void materializeCongress(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.table +"_congress_sample " + " SELECT * FROM " + this.table +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.table +"_congress_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.table +"_congress_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.congress.get(i) + " AS sf"
					+ " FROM "+ this.table + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.congress.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeCongress : "+sql);
			
			new DBUtil().excute(sql);
			
		}
	}
	
	/**
	 * 物化基于senate抽样技术得到的样本。
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_senate_sample(原始属性集, sf)
	 */
	public void materializeSenate(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.table +"_senate_sample " + " SELECT * FROM " + this.table +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.table +"_senate_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.table +"_senate_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.senate.get(0) + " AS sf"
					+ " FROM "+ this.table + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.senate.get(0)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeSenate : "+sql);
			new DBUtil().excute(sql);
		}
	}
	/**
	 * 物化基于house抽样技术得到的样本。
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_house_sample(原始属性集, sf)
	 */
	public void materializeHouse(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.table +"_house_sample " + " SELECT * FROM " + this.table +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.table +"_house_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.table +"_house_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.house.get(i) + " AS sf"
					+ " FROM "+ this.table + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.house.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeHouse : "+sql);
			new DBUtil().excute(sql);
			
		}
	}
	
	/**
	 * 物化基于basic congress抽样技术得到的样本。
	 * 对原始数据进行取样
	 * 将原始数据存储在数据库当中
	 * 样本表：原始表名_basic_congress_sample(原始属性集, sf)
	 */
	public void materializeBasicCongress(){
		//为每一个组构造一个sql语句，来取样
		String sql = null;
		StringBuilder attrs = null;
		
		new DBUtil().excute("CREATE TABLE " + this.table +"_basic_congress_sample " + " SELECT * FROM " + this.table +" LIMIT 0 ");
		new DBUtil().excute("ALTER TABLE " + this.table +"_basic_congress_sample ADD COLUMN sf decimal(16,6)");
		for(int i = 0; i < this.groupNum; i++){
			attrs = new StringBuilder();
			// TODO 以后增添分组属性类型的判断，判断值类型的格式应该怎么拼接
			for(int j = 0; j < this.groupingAttrs.size(); j++){
				attrs.append(this.groupingAttrs.get(j) + "='" + this.groupingValues.get(i).get(j) + "' AND ");
			}
			sql = "INSERT INTO " + this.table +"_basic_congress_sample ";
			// TODO 不论是等距抽样还是随机抽样，都存在样本表中样本量与用户指定的样本大小的一定的出入，如何修正这一点？
			// 组内的随机抽样
			sql +=  "SELECT l.*, "+ this.tupleNumOfGrouping.get(i)/this.basicCongress.get(i) + " AS sf"
					+ " FROM "+ this.table + " AS l "
					+ " WHERE " + attrs.substring(0,attrs.lastIndexOf("AND")) 
					+ " AND rand() < " + this.basicCongress.get(i)/this.tupleNumOfGrouping.get(i);
			
			System.out.println("materializeBasicCongress : "+sql);
			new DBUtil().excute(sql);
		}
	}
	
}
