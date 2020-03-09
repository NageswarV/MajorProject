package com.mlrit.subject_analysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubjectAnalysisReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException {
		int pass = 0, adv_supp = 0, one_to_4_attempts = 0, moreThan4 = 0,total=0;
		int failCount;
		String key=word.toString();
		if(key.contains("Subject")) {
		for (Text value : values) {
			total++;
			failCount = 0;
			String line = value.toString();
			String[] words = line.split(",");
			int index = 0;
			while (index<words.length && !words[index].equals("P")) {
				String cell=words[index];
				if(cell!=null) {
				if (cell.equalsIgnoreCase("F") || cell.equalsIgnoreCase("fail")) {
					failCount++;
				}
				}
				index++;
			}
			if(failCount==0)
				pass++;
			else if(failCount==1)
				adv_supp++;
			else if(failCount<=4)
				one_to_4_attempts++;
			else
				moreThan4++; 
		}
		String outValue="\n    Pass percentage                  :"+Math.round((((float)pass/total)*100)*100.00/100.00)+"%";
	       outValue+="\nStudents passed in 1st Attempt       :"+pass;
	       outValue+="\nStudents passed in 2st Attempt       :"+adv_supp;
	       outValue+="\nStudents passed in 1-4 Attempts      :"+one_to_4_attempts;
	       outValue+="\nStudents passed in 5 or more Attempts:"+moreThan4;
	       outValue+="\n Total Students 				:"+total; 
	con.write(word,new Text(outValue));
		}
		else {
			for(Text value: values) {
				String []arr=value.toString().split(",");
				pass+=Integer.parseInt(arr[0]);
				adv_supp+=Integer.parseInt(arr[1]);
				one_to_4_attempts+=Integer.parseInt(arr[2]);
				moreThan4+=Integer.parseInt(arr[3]);
				total+=Integer.parseInt(arr[4]);
			}
			String outValue="\n    Pass percentage                  :"+Math.round(((pass/total)*100)*100.0/100.0);
			       outValue="\nStudents passed in 1st Attempt       :"+pass;
			       outValue="\nStudents passed in 2st Attempt       :"+pass;
			       outValue="\nStudents passed in 1-4 Attempts      :"+pass;
			       outValue="\nStudents passed in 5 or more Attempts:"+pass;
			       outValue="\n Total Students 						:"+total; 
			con.write(word,new Text(outValue));
		}
	}
}
