package com.mlrit.cgpa;


import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CGPAMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
	float std_cgpa;
	StringBuffer details;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		float filter=context.getConfiguration().getFloat("filter",1.0f);
		String record[]=value.toString().split(",");
		if(record.length>=20 && Character.isDigit(record[20].charAt(0))) {
		std_cgpa=Float.parseFloat(record[20]);
		if(std_cgpa>=filter) {
		
			details=new StringBuffer(record[0]+" "+record[1]);
			while(details.length()<=40)
				details.append(" ");
			context.write(new Text(details.toString()), new FloatWritable(std_cgpa));
		}
		}
	}

}
