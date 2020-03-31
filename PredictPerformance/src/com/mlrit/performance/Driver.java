package com.mlrit.performance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mlrit.performance.predict.PredictionDriver;



public class Driver {

	public static void main(String[] args)throws IOException,ClassNotFoundException,InterruptedException {
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path tmpoutput=new Path(files[1]);
		String finalOut=files[2];
		c.set("rollNumber", files[3]);
		@SuppressWarnings("deprecation")
		Job j=new Job(c,"predictPerformance");
		FileInputFormat.setInputDirRecursive(j, true);
		j.setJarByClass(PerformancePredictMapper.class);
		j.setMapperClass(PerformancePredictMapper.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, tmpoutput);
		if(j.waitForCompletion(true)) {
			
			if(PredictionDriver.predict(files[0],files[1],finalOut)) {
				System.exit(0);
			}
		}
		System.exit(1);
	}

}
