package com.mlrit.backlogs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BacklogMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	StringBuffer details;
	int std_backlogs;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		int filter=context.getConfiguration().getInt("filter",0);
		String record[]=value.toString().split(",");
		if(record.length>=22 && Character.isDigit(record[21].charAt(0))) {
		std_backlogs=Integer.parseInt(record[22]);
		if(std_backlogs>=filter) {
		
			details=new StringBuffer(record[0]+" "+record[1]);
			while(details.length()<=40)
				details.append(" ");
			context.write(new Text(details.toString()), new IntWritable(std_backlogs));
		}
		}
	}

}
