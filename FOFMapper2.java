package fof;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FOFMapper2 extends Mapper<LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value,Context context)throws IOException,InterruptedException{
		String[] val = value.toString().split("\t");
		String name = val[0].split(",")[0];
		context.write(new Text(name), new Text(val[0].split(",")[1]+","+val[1]));
	}
}
