# MajorProject
-------------------------------
Input file Type: .csv
-------------------------------
StudentAnalysis from directory Backlogs&CGPA (CGPA , Final Backlogs , Final Credits)
SubjectAnalysis from Unstructered Data Set (OverAnalysis of all subjects, single subject analysis)
....................................................
StudentAnalyze.jar  (with three packages )
SubjectAnayze.jar   (with two packages  )
.....................................................
studentAnalyze.jar
--------------------
hadoop jar /home/16r2csec/vnr/StudentAnalyze.jar com.mlrit.[cgpa or backlogs or credits].Driver <inputdirectory>(vnr) <outputdirectory> <inputfield>(base cgpa or credit or backlog)
.............................................................
SubjectAnalyze.jar
-----------------------------------------
  hadoop jar /home/16r2csec/vnr/SubjectAnalyze.jar com.mlrit.[subject_analysis or overviewofsubjects].Driver <inputfile-sheet1> <inputfile-sheet2> <outputdirectory> <subjectCode or subjectName>
  
  


