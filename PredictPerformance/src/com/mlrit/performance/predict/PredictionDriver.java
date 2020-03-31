package com.mlrit.performance.predict;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PredictionDriver {

	public static boolean predict(String indataPath,String queryDataPath,String outputPath) throws IOException,InterruptedException,ClassNotFoundException{
		Configuration c=new Configuration();
		Path input=new Path(indataPath);
		Path output=new Path(outputPath );
		c.set("studentData", getStudentInfo(queryDataPath));
		@SuppressWarnings("deprecation")
		Job j=new Job(c,"predictPerformance");
		FileInputFormat.setInputDirRecursive(j, true);
		j.setJarByClass(PredictionMapper.class);
		j.setMapperClass(PredictionMapper.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		if( j.waitForCompletion(true)) {
			return true;
		}
		else {
			return false;
		}
	}
	private static String getStudentInfo(String inputFile)throws IOException,InterruptedException{
		
		String uri = "hdfs://172.16.103.61:8020/";
		String user = "16r2csec";
		FileSystem fs = FileSystem.get(URI.create(uri), new Configuration(), user);
		FSDataInputStream in=fs.open(new Path(inputFile+"/part-00000"));
		BufferedReader br=new BufferedReader(new InputStreamReader(in));
		String studentInfo=br.readLine().trim();
		return studentInfo;
	}

}
