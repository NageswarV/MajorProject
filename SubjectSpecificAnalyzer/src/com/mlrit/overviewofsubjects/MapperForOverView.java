package com.mlrit.overviewofsubjects;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForOverView extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] record=line.split(",");
		if(record.length>4) {
		String subject=record[1];
		String subjectCode=record[0];
		if(!(subjectCode.contains("REGISTER") || subjectCode.equalsIgnoreCase("TABULATION REGISTER") || subjectCode.equalsIgnoreCase("sub code"))) {
		  Text outputKey = new Text(subjectCode+" "+subject.toUpperCase().trim()+" Subject");
		  Text outputValue = new Text(record[4]);
		  context.write(outputKey, outputValue);
		}
		}
	
	}
}
