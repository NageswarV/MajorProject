package com.mlrit.subject_analysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubjectAnalysisMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words=line.split(",");
		if(words.length>=1) {
		String subject=context.getConfiguration().get("subject");
		String word=words[1];
		String subjectCode=words[0];
		if(subject.equalsIgnoreCase(word) || subject.equalsIgnoreCase(subjectCode)) {
		  Text outputKey = new Text(word.toUpperCase().trim()+" Subject");
		  Text outputValue = value;
		  context.write(outputKey, outputValue);
		}
		}
	}

}
