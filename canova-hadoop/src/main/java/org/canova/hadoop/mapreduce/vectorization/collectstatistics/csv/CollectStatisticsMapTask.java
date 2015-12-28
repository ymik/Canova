package org.canova.hadoop.mapreduce.vectorization.collectstatistics.csv;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.canova.hadoop.csv.schema.CSVInputSchema;
import org.canova.hadoop.csv.schema.CSVSchemaColumn;
import org.canova.hadoop.utils.CanovaUtils;
import org.canova.hadoop.utils.ConfTools;


public class CollectStatisticsMapTask extends Mapper<LongWritable, Text, Text, Text> {

    //private Matcher logRecordMatcher;
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    // where is the text of the schema loaded?
    //CSVInputSchema schema = null;
    
    //private Text outputValue = new Text();
    //Schema[] schemas = null; // new Schema[];
    //Map<String, Schema> inputDirectoryToSchemaMap = new LinkedHashMap<String, Schema>();

    String delimiter = ",";
    CSVInputSchema csvSchema = null;
    boolean skipHeader = false;
    long recordsSeen = 0;
    
    /**
     * get the input schemas for the vectors loaded
     * 
     * 
     * TODO: Load the csv schema from somewhere
     * 
     */
    protected void setup(Context context) throws IOException, InterruptedException {
    		
    	Configuration conf = context.getConfiguration();
    	System.out.println( "Map::setup() method -----" );
    	
    	
    	String wfConfVal = conf.get("oozie.action.id" );
    	if ( null != wfConfVal ) {
    		    		
    		System.out.println( "We're using Oozie" );

    		// INPUT_VECTOR_SCHEMA_FILENAME_KEY
    		String schemaPath = conf.get( CanovaUtils.INPUT_VECTOR_SCHEMA_FILENAME_KEY );
    		if (null == schemaPath) {
    			System.err.println( "No valid input vector schema!" );
    			return;
    		}
    		
    		System.out.println( "schema path: " + schemaPath );
    		
    		try {
    			
    			// oozie hack
    			ConfTools.loadTextFileContentsIntoConf( conf, CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY, schemaPath );
    			System.out.println( schemaPath + " schema file contents loaded into conf..." );
    			
    		} catch (Exception e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}    		
    		
    		
    	}
    	
    	
    	
		String contents = conf.get( CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY );
//		System.out.println( "CSV Schema Contents: " + contents );
    	
    	String skipHeaderValue = conf.get( CollectStatisticsMapReduceJob.SKIP_HEADER_KEY, "false" );
    	if ("true".equals(skipHeaderValue.trim().toLowerCase())) {
    		this.skipHeader = true;
    	}
    	
    	System.out.println("Skip Header? " + this.skipHeader);
	
		this.csvSchema = new CSVInputSchema();
		try {
			this.csvSchema.parseSchemaFromRawText(contents);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }
    
    /**
     * 
     * 
     * split the line on the delimiter
     * 
     * 
     * 
     * emit keys for: { column.name } // emit the name for each column name based on the column position of the value
     * emit value: V1 { value } // original value
     * 
     * 
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	// 1. figure out which schema the record is from
    	
    	String csvLine = value.toString();
    	String[] columns = csvLine.split( this.csvSchema.delimiter );


    	String keyParsed = null;

    	if (key.get() == 0 && this.skipHeader) {
    		
    		// skip header!
    		System.out.println( "Skipping header: " + csvLine );
    	
    	} else if (columns[0].trim().equals("")) {

    		
    	} else {
    		
    		int srcColIndex = 0;
		    int dstColIndex = 0;

		    //log.info( "> Engine.vectorize() ----- ");

		    double label = 0;    		
    	
	
	    	// FOR EACH KEY in the column set
	        // scan through the columns in the schema / input csv data
	        for (Map.Entry<String, CSVSchemaColumn> entry : this.csvSchema.getColumnSchemas().entrySet()) {
	
	          String schemaColumnKey = entry.getKey();
	          CSVSchemaColumn schemaColumnEntry = entry.getValue();
	          
	          outputKey.set( schemaColumnKey.trim() );
	          
	          outputValue.set( columns[ srcColIndex ] ); 
	          
	          context.write( outputKey, outputValue );
	          
	          srcColIndex++;
	          
	        }
	    
	        this.recordsSeen++;	
	        
    	}
    	
        
        	
  //      	
    	
    }
    
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		
		System.out.println( "Mapper > Records Seen: " + this.recordsSeen );
		
    }	
    
}
