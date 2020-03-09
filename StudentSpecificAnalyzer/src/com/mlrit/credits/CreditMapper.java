package com.mlrit.credits;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CreditMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	StringBuffer details;
	int std_credits;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int baseCredit=context.getConfiguration().getInt("baseCredits",22);

		String record[]=value.toString().split(",");
		if(record.length>=21 && Character.isDigit(record[21].charAt(0))) {
		std_credits=Integer.parseInt(record[21]);
		if(std_credits<=baseCredit) {
		
			details=new StringBuffer(record[0]+" "+record[1]);
			while(details.length()<=40)
				details.append(" ");
			context.write(new Text(details.toString()), new IntWritable(std_credits));
		}
		}
	}

}
