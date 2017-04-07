package mondrian.sampling;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import db.DBUtil;

public class CongressionalSamplesTest {
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

	/**
	 * 基于样本数据计算count聚集操作的误差
	 */
	@Test
	public void computeCongressError(){
		String sql = "SELECT avg(zipfNum) FROM test_congress_sample GROUP BY group_one ";
		String summarySql = "SELECT avg(zipfNum) FROM test GROUP BY  group_one";
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
		String sql = "SELECT avg(zipfNum) FROM test_basic_congress_sample GROUP BY group_one ";
		String summarySql = "SELECT avg(zipfNum) FROM test GROUP BY  group_one";
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
		String sql = "SELECT avg(zipfNum) FROM test_house_sample GROUP BY group_one ";
		String summarySql = "SELECT avg(zipfNum) FROM test GROUP BY  group_one";
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
		String sql = "SELECT avg(zipfNum) FROM test_senate_sample GROUP BY group_one ";
		String summarySql = "SELECT avg(zipfNum) FROM test GROUP BY  group_one";
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
	
}
