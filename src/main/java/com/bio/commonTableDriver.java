package com.bio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.mahout.text.wikipedia.XmlInputFormat;

public final class commonTableDriver {
 public static void main(String[] args) throws Exception {
  commonRunJob(args[0], args[1]);
 }

 public static void commonRunJob(String input,String output) throws Exception  {
  Configuration conf = new Configuration();
  conf.set("xmlinput.start", "<response>");
  conf.set("xmlinput.end", "</response>");
  
  Job job = new Job(conf);
  job.setJarByClass(commonTableMapper.class);
  
  job.setInputFormatClass(XmlInputFormat.class);
  
  job.setMapperClass(commonTableMapper.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Text.class);
  
  job.setNumReduceTasks(0);
  
  FileInputFormat.setInputPaths(job, new Path(input));
  Path outPath = new Path(output);
  FileOutputFormat.setOutputPath(job, outPath);
  
  outPath.getFileSystem(conf).delete(outPath, true);
  job.waitForCompletion(true);
  
 }
}