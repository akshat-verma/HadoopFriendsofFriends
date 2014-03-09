package fof;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FOFReducer2 extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
		int max = 0;
		ArrayList<String> topList = new ArrayList<String>();
		String top = "";
		for(int i=0;i<3;i++){
		ArrayList<String> fields = new ArrayList<String>();
		for(Text line : values) {
			if(!line.toString().split(",")[0].equals(top)) {
			for(int k=0; k < line.toString().split(",").length;k++){
				fields.add(line.toString().split(",")[k]);
			}
					
			int common = Integer.parseInt(fields.get(1));
			if(common > max){
				max =common;
				top = fields.get(0);
			}
		if(!top.isEmpty())
			topList.add(top);
			}
		}
		}
		StringBuffer s = new StringBuffer();
		for(int j=0; j< topList.size();j++){
			if(!topList.get(j).isEmpty())
				s.append(topList.get(j));
				s.append(",");
			
		}
			
		context.write(key,new Text(s.toString()));
	}
	
	
}
