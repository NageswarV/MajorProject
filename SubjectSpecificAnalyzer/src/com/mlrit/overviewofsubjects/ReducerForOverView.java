package com.mlrit.overviewofsubjects;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForOverView extends Reducer<Text, Text, Text, Text>{
	private boolean headerflagbit=true;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if(headerflagbit) {
		context.write(new Text("SubjectCode   subject  "), new Text("\t\t\t   Pass-percentage  pass-count  fail-count  no-of students  "));
		headerflagbit=false;
		}
		float fail=0,total=0;
		if(key.toString().contains("Subject")) {
			int endIndex=key.toString().indexOf("Subject")>50?50:key.toString().indexOf("Subject");
			StringBuffer subject=new StringBuffer(key.toString().substring(0,endIndex));
			for(Text result : values) {
				total++;
				if(result.toString().equalsIgnoreCase("F"))
					fail++;
			}
			while(subject.length()<50) {
				subject.append(" ");
			}
			String outValue="\t"+Math.round(((total-fail)/total)*100);
			outValue+="%\t"+(total-fail);
			outValue+="\t\t"+fail;
			outValue+="\t\t"+total;
		context.write(new Text(subject.toString()),new Text(outValue));
		}
	}

}
