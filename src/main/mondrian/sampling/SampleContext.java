package mondrian.sampling;

import java.util.ArrayList;
import java.util.List;

public class SampleContext {
	
	//查询过程中需要使用样本表的表
	private static List<String> tablesNeededSampling;
	
	static {
		tablesNeededSampling = new ArrayList<String>();
	}
	
	public static void setTablesNeededSampling(List<String> tables){
		for(int i =0 ; i< tables.size(); i++){
			tables.set(i, tables.get(i).toLowerCase());
		}
		tablesNeededSampling = tables;
	}
	public static List<String> getTablesNeededSampling(){
		return tablesNeededSampling;
	}
}
