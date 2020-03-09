package com.mlrit.overviewofsubjects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

	public static void main(String[] args) throws Exception {


Configuration c=new Configuration();
String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
Path input=new Path(files[0]);
Path input2=new Path(files[1]);
Path output=new Path(files[2]);
@SuppressWarnings("deprecation")
Job j=new Job(c,"SubjectAnalysis");
j.setJarByClass(MapperForOverView.class);
j.setMapperClass(MapperForOverView.class);
j.setReducerClass(ReducerForOverView.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(j, input);
FileInputFormat.addInputPath(j,input2);
FileOutputFormat.setOutputPath(j, output);
System.exit(j.waitForCompletion(true)?0:1);
		    
	}
}
