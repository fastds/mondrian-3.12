package mondrianTest;  
  
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Member;
import org.pentaho.ui.xul.samples.SampleEventHandler;

import mondrian.sampling.SampleContext;
import mondrian.sampling.SampleInfoReader;  
  
public class Olap4jTest {         
    /** 
     * 获取连接Olap的连接 
     * @param url  连接Olap的URL 
     * @return 
     * @throws ClassNotFoundException 
     * @throws SQLException 
     */  
    public static OlapConnection getConnection(String url) throws ClassNotFoundException, SQLException{  
        Class.forName("mondrian.olap4j.MondrianOlap4jDriver");  
        Connection connection = DriverManager.getConnection(url);  
        OlapConnection olapConnection = connection.unwrap(OlapConnection.class);  
        return olapConnection;  
    }  
      
    /** 
     * 获取查询的结构结果集 
     * @param mdx  mdx查询语句 
     * @param conn Olap连接 
     * @return 
     * @throws OlapException 
     */  
    public static CellSet getResultSet(String mdx,OlapConnection conn) throws OlapException{  
        OlapStatement statement = conn.createStatement();  
        CellSet cellSet = statement.executeOlapQuery(mdx);  
        return cellSet;  
    }  
      
     public void testQuery(){                 
         OlapConnection connection = null;  
         //加载样本元信息
         SampleInfoReader.loadSampleInfo();
         //设置需要从样本进行查询的表
         List<String> tables = new ArrayList();
         tables.add("inventory_fact_1997");
         SampleContext.setTablesNeededSampling(tables);
         
         System.out.println(SampleInfoReader.sampleInfos);
        try {  
            connection = getConnection("jdbc:mondrian:" +   
                            "Jdbc=jdbc:mysql://localhost:3306/foodmart?user=root&password=zhouyu4444;" +  
                            "Catalog=E:\\oxygen_workspace\\mondrian\\FoodMart.xml;");  
        } catch (ClassNotFoundException e1) {  
            e1.printStackTrace();  
        } catch (SQLException e1) {  
            e1.printStackTrace();  
        }  
//        String query = "SELECT  { [Measures].[Unit Sales] } on columns,{ [Time].[Year].[1997] } on rows FROM Sales  WHERE ([Customers].[State Province].[CA])";  
//        String query = "SELECT {[Measures].[Store Sqft]} on columns FROM Store";
        String query = "SELECT {[Measures].[Warehouse Sales]} ON COLUMNS, {[Store Type].[All Store Types]} ON ROWS FROM [Warehouse]";
        //获取查询结果    
        CellSet cs = null;  
        try {  
            cs = getResultSet(query, connection);  
        } catch (OlapException e) {  
            e.printStackTrace();  
        }   
  
        PrintWriter pw = new PrintWriter(System.out);  
  
        //处理返回数据  
        if(cs.getAxes().size()>1){  
            for (Position row : cs.getAxes().get(1)) {  
                for (Position column : cs.getAxes().get(0)) {  
                    for (Member member : row.getMembers()) {  
                        System.out.println("rows:"+member.getUniqueName());  
                    }  
                    for (Member member : column.getMembers()) {  
                        System.out.println("columns:"+member.getUniqueName());  
                    }  
                    final Cell cell = cs.getCell(column, row);  
                    System.out.println("values:"+cell.getValue());  
                    System.out.println();
                }
                }  
        }else{  
            for(Position column:cs.getAxes().get(0))  
            {  
                for(Member member:column.getMembers()){  
                    System.out.println("columns:"+member.getUniqueName());  
                }  
                Cell cell=cs.getCell(column);  
                System.out.print("values:"+cell.getValue());  
                System.out.println();  
            }  
        }  
  
    }  
  
    public static void main(String[] args) {  
        Olap4jTest a =  new Olap4jTest();  
        System.out.println("调用mondrian api进行查询");
        a.testQuery();  
    }  
} 