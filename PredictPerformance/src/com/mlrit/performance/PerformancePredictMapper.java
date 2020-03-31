package com.mlrit.performance;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerformancePredictMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String rollNumber=context.getConfiguration().get("rollNumber");
		String record[]=value.toString().split(",");
		int i=2;
		String studentDetails=rollNumber;
		if(record.length>2) {
			if(record[0].equalsIgnoreCase(rollNumber)) {
				while(i<record.length) {
					studentDetails+=","+record[i];
					i+=2;
				}
				context.write(new Text(""), new Text(studentDetails));
			}
		}
	}

}
