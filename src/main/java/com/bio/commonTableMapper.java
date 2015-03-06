package com.bio;

import com.bio.xmlDef;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class commonTableMapper 
 extends org.apache.hadoop.mapreduce.Mapper {
  private HTable htable;
  
 // create HBase connection
 protected void setup(Context context) 
    throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    htable = new HTable(conf, xmlDef.xmlDef[0][0]);
    htable.setAutoFlush(true);
    htable.setWriteBufferSize(1024 * 1024 * 12);
    }
   
 public void map(LongWritable key, Text value, Mapper.Context context) 
  throws IOException, InterruptedException {
   String currLine = value.toString();
   try {
    XMLStreamReader reader = XMLInputFactory.newInstance()
    .createXMLStreamReader(
      new ByteArrayInputStream(currLine.getBytes()));
 
    String currentElement = "";
    int col = 0;
    
    // initialize all xml value to blank string
    for (String xmlTag : xmlDef.xmlDef[3]) {
     xmlDef.xmlDef[5][col] = "";
     col++;
    }
    
    
    // read xml tags and store values in xmlDef
    while (reader.hasNext()) {
     int code = reader.next();
     switch (code) {
     case START_ELEMENT:
      currentElement = reader.getLocalName();
      break;
     case CHARACTERS:
      col = 0;
      for (String xmlTag : xmlDef.xmlDef[3]) {
       if (currentElement.equalsIgnoreCase(xmlTag)) {
        xmlDef.xmlDef[5][col] += reader.getText().trim(); 
       }
       col++;
      }
     }
    }
    
    
    // writing values to mapper output file
    // can remove this context.write
    context.write(xmlDef.xmlDef[5][0]+"#"+xmlDef.xmlDef[5][1]+"#"+xmlDef.xmlDef[5][2]+"#"+xmlDef.xmlDef[5][3]+"#"+xmlDef.xmlDef[5][4],1);
    
    // put record in HBase
    Put insHBase = new Put(Bytes.toBytes(xmlDef.xmlDef[5][0]));
    col = 0;
    for (String xmlTag : xmlDef.xmlDef[3]) {
     insHBase.add(Bytes.toBytes(xmlDef.xmlDef[2][col]), Bytes.toBytes(xmlDef.xmlDef[3][col]), Bytes.toBytes(xmlDef.xmlDef[5][col]));
     col++;
    }
    htable.put(insHBase);
  
  } catch (Exception e) {
    e.printStackTrace();
  }
 }
  @Override
  public void cleanup(Context context) 
   throws IOException, InterruptedException {
   htable.flushCommits();
   htable.close();
   
 }
}