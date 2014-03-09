package fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Logger logger = Logger.getLogger("sds");
		String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();

		Job job_init = new Job(conf, "Recommend");
		job_init.setJarByClass( Driver.class );
		job_init.setMapperClass(FOFMapper1.class);
		job_init.setReducerClass(FOFReducer1.class);
		job_init.setMapOutputKeyClass( Text.class );
		job_init.setMapOutputValueClass( IntWritable.class );
		job_init.setOutputKeyClass( Text.class );
		job_init.setOutputValueClass(IntWritable.class );
		FileInputFormat.addInputPath( job_init, new Path( otherArgs[0] ) );
		FileOutputFormat.setOutputPath(job_init, new Path( otherArgs[1]+"_temp") );
		job_init.waitForCompletion(true);
		Job job_final = new Job(conf, "Recommend");
		job_final.setJarByClass( Driver.class );
		job_final.setMapperClass(FOFMapper2.class);
		job_final.setReducerClass(FOFReducer2.class);
		job_final.setMapOutputKeyClass( Text.class );
		job_final.setMapOutputValueClass( Text.class );
		job_final.setOutputKeyClass( Text.class );
		job_final.setOutputValueClass(Text.class );
		FileInputFormat.addInputPath( job_final, new Path( otherArgs[1]+"_temp"+"/part-r-00000" ) );
		FileOutputFormat.setOutputPath(job_final, new Path( otherArgs[1]) );
		job_final.waitForCompletion(true);
		
	}

}
