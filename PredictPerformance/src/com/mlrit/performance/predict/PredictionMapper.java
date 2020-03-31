package com.mlrit.performance.predict;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictionMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			String[] currentRecord=value.toString().split(",");
			String[] studentData=context.getConfiguration().get("studentData").split(",");
			String rollNumber=studentData[0];
			int incr=2,index=1;
			if(Integer.parseInt(currentRecord[0].substring(0,2))<=Integer.parseInt(rollNumber.substring(0,2))) {
				while(index<studentData.length && incr<currentRecord.length-5) {
					if(Math.abs(Float.parseFloat(currentRecord[incr])-Float.parseFloat(studentData[index++]))<1){
						incr+=2;
					}
					else {
						break;
					}
					
				}
				if(index>=studentData.length) {
						context.write(new Text(rollNumber), value);
					}
				
			}
	}
	
}
