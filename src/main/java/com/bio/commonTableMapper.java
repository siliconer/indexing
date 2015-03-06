package com.bio;

import com.bio.xmlDef;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
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
   String currentElement = "";
   try {
	   String root_dir = System.getProperty("user.dir");
		String file_path = root_dir + File.separator + "src/xmlRead"+File.separator + "DRA002064.experiment.xml"  ;
		System.out.println(file_path);
//	    XMLStreamReader reader = XMLInputFactory.newInstance()
//	    .createXMLStreamReader(
//	      new ByteArrayInputStream(currLine.getBytes()));
	    XMLStreamReader streamReader = XMLInputFactory.newInstance()
	    .createXMLStreamReader(new FileReader(file_path));
	    int col = 0 ;   
	    for (String xmlTag : xmlDef.xmlDef[3]) {
	        xmlDef.xmlDef[5][col] = "";
	        col++;
	       }
	    col = 0;
//	    while(streamReader.hasNext()) {
//	    	System.out.println(streamReader.getEventType());
//	    	if (streamReader.getEventType() == 1) {
//	    		System.out.println(streamReader.getLocalName());
//	    	} else if (streamReader.getEventType() == 4) {
//	    		System.out.println(streamReader.getText());
//	    	}
//	    		
//	    	streamReader.next();
//	    }

	    while(streamReader.hasNext()) {
	    	if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
	    		currentElement = streamReader.getLocalName();
	    		if (currentElement == "SUBMITTER_ID") {
	    			System.out.println(streamReader.getAttributeValue(null,"namespace"));
		    	   	xmlDef.xmlDef[5][1] = streamReader.getAttributeValue(null,"namespace"); 

	    		}

	    	} else if (streamReader.getEventType() == XMLStreamReader.CHARACTERS) {
	    		 for (String xmlTag : xmlDef.xmlDef[4]) {
//		    			  	System.out.println(streamReader.getText() +" value");
//		    			    System.out.println(xmlTag+" tag");
	    		       if (currentElement.equals(xmlTag)) {
	    		    	   if (xmlTag.equals("SUBMITTER_ID"))  {
	    		    		   col++;
	    		    		   break;
	    		    	   }
	    		    	   System.out.println(col);
	    		    	   xmlDef.xmlDef[5][col] = streamReader.getText(); 
	    		    	   System.out.println(xmlDef.xmlDef[5][col]);
	    		    	   col++;
		    		       if (col >11){
		    		    	   break;
		    		       }
		    		       break;
	    		       }
	    		    
	    		 }
	    		 System.out.println(currentElement +" "+  streamReader.getText());
 		       	currentElement = "";

	    	}
	    	streamReader.next();
	    	
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