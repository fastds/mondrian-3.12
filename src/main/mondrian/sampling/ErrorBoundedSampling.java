package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DBUtil;

/**
 * 根据用户指定的误差和置信度，计算需要进行采样大小。计算过程如下：
 * 1、基于用户指定的分组属性查询数据库，查询用户所指定的聚集列的值
 * 2、计算每个分组的buckets，计算各个bucket的采样率
 * 3、根据所有bucket和对应的采样率，对原始表的进行随机抽样
 * 4、物化样本
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
	private float error ;				//误差
	private float confidence;			//置信度
	private String table;				//原始表
	private List<String> groupingAttrs;	//分组属性
	private List<String> aggregateAttrs;	//聚集属性
	
	public ErrorBoundedSampling(float error, float confidence
			,String table, List<String> groupingAttrs, List<String> aggregateAttrs){
		this.error = error;
		this.confidence = confidence;
		this.table = table;
		this.groupingAttrs = groupingAttrs;
		this.aggregateAttrs = aggregateAttrs;
		
		load();
	}
	/**
	 * 从table中加载属性
	 */
	public void load(){
		
		StringBuilder loadSql = new StringBuilder();
		StringBuilder grpAttrsStr = new StringBuilder();
		
		for(String temp:this.groupingAttrs){
			grpAttrsStr.append(temp+", ");
		}
		
		loadSql.append("SELECT "+ aggregateAttrs.get(0) +" FROM "+ this.table +" GROUP BY " + grpAttrsStr.substring(0,grpAttrsStr.lastIndexOf(",")));
		
		DBUtil db = new DBUtil();
		ResultSet rs = null;
		rs = db.excuteQuery(loadSql.toString());
		
		try {
			while(rs.next()){
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
