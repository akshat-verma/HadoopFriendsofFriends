package fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FOFReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
		int sum=0;
		for (IntWritable val:values){
			if( val.get()==0)	break;
			else sum=sum+val.get();
		}
		context.write(key,new IntWritable(sum));
		
	}
	
}


	
