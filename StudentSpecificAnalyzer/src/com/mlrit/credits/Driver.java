package com.mlrit.credits;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {

	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		//Path input2=new Path(files[1]);
		Path output=new Path(files[1]);
		c.set("baseCredits", files[2]);
		@SuppressWarnings("deprecation")
		Job j=new Job(c,"studentsAnalysis");
		FileInputFormat.setInputDirRecursive(j, true);
		j.setJarByClass(CreditMapper.class);
		j.setMapperClass(CreditMapper.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		//FileInputFormat.addInputPath(j, input2);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

}
