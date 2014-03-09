package fof;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class FOFMapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{
	 public void map(LongWritable key, Text value,  Context context ) throws IOException, InterruptedException  {
	      
	      String line = value.toString();
	      StringBuffer neighbours = new StringBuffer();
	      ArrayList<String> fields = new ArrayList<String>();
	      for(int i=0 ; i < line.split(",").length ; i++) {
	    	  if(!line.split(",")[i].isEmpty()) fields.add((line.split(","))[i]);
	      }
	      String name = fields.get(0);
	      for(int i=0 ; i<fields.size() ; i++) {
	    	  context.write(new Text(name+","+fields.get(i)),new IntWritable(0));
	      }
	      for(int i=0 ; i<fields.size()-1 ; i++) {
	    	  for(int j=1; j <fields.size() ; j++)
	    	  context.write(new Text(fields.get(i)+","+fields.get(j)),new IntWritable(1));
	      }
	      
	      }
}


