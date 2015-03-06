package com.bio;

public class xmlDef {
	 public static String xmlDef[][] = new String[][]{
	      {"xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest", "xmlTest"},     //HBase table name
	      {"Y", "N", "N","N","N","N", "N", "N","N","N","N"},                                            //is column a key column?
	      {"Identifiers", "Identifiers","title","study_ref","sample","libraray","libraray","libraray","platform","pipeline","pipeline"},                                 //column family
	      {"primary_id", "submitter_id", "title","primary_id", "sample_id", "library_strategy", "library_source", "library_selection", "instrument_model", "step_index","prev_step_index"}, //column name in HBase
	      {"PRIMARY_ID", "SUBMITTER_ID","TITLE","PRIMARY_ID", "LIBRARY_STRATEGY", "LIBRARY_SOURCE","LIBRARY_SELECTION","INSTRUMENT_MODEL","STEP_INDEX","PREV_STEP_INDEX"}, //xml tag
	      {"", "", "", "", "", "","", "", "", "", ""}                                                // place holder for xml value
	      };
	}
