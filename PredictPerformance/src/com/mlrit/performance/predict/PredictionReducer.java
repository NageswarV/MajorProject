package com.mlrit.performance.predict;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictionReducer extends Reducer<Text,Text,Text,Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> students=values.iterator();
		String flag1="",flag2="",result="";
		int index=0,count=0;
		
		String student_given[]=key.toString().split(",");
		for(int i=1;i<student_given.length-1;i++) {
			float sem_marks=Float.parseFloat(student_given[i]);
			float nextSem=Float.parseFloat(student_given[i+1]);
			index++;
			float diff=sem_marks-nextSem;
			if(diff>0.5) {
				flag1+=1;
			}else if(diff>0) {
				flag1+=2;
			}else if(diff>-0.5) {flag1+=3;}
			else {flag1+=4;}
			
		}

		float marks[]=new float[8];
		Arrays.fill(marks, 0);
		while(students.hasNext()) {
			
			String studentData[]=students.next().toString().trim().split(",");
			for(int i=1;i<student_given.length-1;i++) {
				float sem_marks=Float.parseFloat(studentData[i]);
				float nextSem=Float.parseFloat(studentData[i+1]);
					float diff=sem_marks-nextSem;
					if(diff>0.5) {
						flag2+=1;
					}else if(diff>0) {
						flag2+=2;
					}else if(diff>-0.5) {flag2+=3;}
					else {flag2+=4;}
			}
			
			if(flag1.equals(flag2)) {
				for(int i=index;i<studentData.length;i++) {
				 marks[i]+=Float.parseFloat(studentData[i]);
				}
				count++;
			}
		}
		for(int i=0;i<8;i++) {
		result+=(marks[i]/count)+"  ";
		}
		context.write(new Text(student_given[0]),new Text(result));
	}

}
