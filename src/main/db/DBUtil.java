package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtil {
	private static Connection con;
	private PreparedStatement statement;
	
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	public static Connection getConnection(){
		try {
			if(con == null)//192.168.181.128 vm psw:123
				con = DriverManager.getConnection("jdbc:mysql://localhost:3306/foodmart", "root", "zhouyu4444");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}
	public DBUtil(){
		try {
			if(this.con == null)
				this.con = DriverManager.getConnection("jdbc:mysql://localhost:3306/foodmart", "root", "zhouyu4444");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public ResultSet excuteQuery(String sql){
		try {
			this.statement = this.con.prepareStatement(sql);
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public boolean excute(String sql){
		try {
			PreparedStatement statement = this.con.prepareStatement(sql);
			return statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public void close(){
		try{
			if(this.statement != null)
				this.statement.close();
			if(this.con != null)
				this.con.close();
		}catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

}
