package org.canova.hadoop.mapreduce.vectorization.csv.vectorize;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.canova.hadoop.csv.schema.CSVInputSchema;
import org.canova.hadoop.mapreduce.vectorization.csv.collectstatistics.CollectStatisticsMapReduceJob;
import org.canova.hadoop.mapreduce.vectorization.csv.derivestatistics.DeriveStatisticsMapReduceJob;
import org.canova.hadoop.utils.CanovaUtils;
import org.canova.hadoop.utils.ConfTools;
import org.canova.hadoop.utils.SVMLightUtils;


public class VectorizeMapTask extends Mapper<LongWritable, Text, NullWritable, Text> {

    //private Matcher logRecordMatcher;
    //private Text outputKey = new Text();
    private Text outputValue = new Text();

    // where is the text of the schema loaded?
    //CSVInputSchema schema = null;
    
    //private Text outputValue = new Text();
    //Schema[] schemas = null; // new Schema[];
    //Map<String, Schema> inputDirectoryToSchemaMap = new LinkedHashMap<String, Schema>();

    //String delimiter = ",";
    CSVInputSchema csvSchema = null;
	
    // not sure we need this --- the presence of the @ATTRIBUTE action !UNIQUE_ID renders the column automatically
	String vectorOutputFormat = "svmlight"; 

	boolean skipHeader = false;

    
    public static String parseColumnName(String pathName) {
    	
    	
    	
    	String[] parts = pathName.split( "/" );
    	
    	// get the last part
    	String fileNamePath = parts[ parts.length - 1 ];
    	
    	
    	
    	String[] fileNameParts = fileNamePath.split("\\.");
    	
    	
    	
    	if (fileNameParts.length < 2) {
    		return "";
    	}
    	
    	return fileNameParts[ 0 ].trim();
    	
    }
    
    private void loadDCacheFileLinesIntoConf(Configuration conf, Path path) {
    	
    	try (BufferedReader br = new BufferedReader(new FileReader(path.toString()))) {
    	    String line;
    	    while ((line = br.readLine()) != null) {
    	       
    	    	if ("".equals(line.trim())) {
    	    		// blank!
    	    	} else {
    	    		String[] parts = line.split("=");
    	    		System.out.println( "Conf > " + parts[ 0 ] + " => " + parts[ 1 ] );
    	    		conf.set(parts[0].trim(), parts[1].trim());
    	    	}
    	    	
    	    }
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    	
    	
    }
    
    /**
     * get the input schemas for the vectors loaded
     * 
     * 
     * TODO: Load the csv schema from somewhere
     * 
     */
    protected void setup(Context context) throws IOException, InterruptedException {
    		
    	Configuration conf = context.getConfiguration();
    	System.out.println( "CSVVectorizie->Map::setup() method -----" );
    	
    	

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
		
		String schemaStatisticsBasePath_Base = conf.get( VectorizeMapReduceJob.VECTOR_SCHEMA_STATS_INPUT_KEY );
        if (!schemaStatisticsBasePath_Base.endsWith("/")) {
        	schemaStatisticsBasePath_Base += "/";
        }

		String schemaStatisticsBasePath = schemaStatisticsBasePath_Base + "schema/stats/";
		
		
		
		String schemaDerivedStatisticsBasePath_Base = conf.get( DeriveStatisticsMapReduceJob.DERIVED_STATS_OUTPUT_KEY );
        if (!schemaDerivedStatisticsBasePath_Base.endsWith("/")) {
        	schemaDerivedStatisticsBasePath_Base += "/";
        }
        
        String schemaDerivedStatisticsBasePath = schemaDerivedStatisticsBasePath_Base + "schema/derived_stats/";

        
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
		
		Path[] localPaths = context.getLocalCacheFiles();
		
		for ( int x = 0; x < localPaths.length; x++ ) {
			
			System.out.println( "DCache Path: " + localPaths[ x ].toString() );
			//this.loadDCacheFileLinesIntoConf( conf, localPaths[x] );
			
			
		}
		
		this.csvSchema.loadSchemaStatisticsFromDistributedCache( conf, localPaths, schemaStatisticsBasePath );
		this.csvSchema.loadSchemaDerivedStatisticsFromDistributedCache( conf, localPaths, schemaDerivedStatisticsBasePath );
		this.csvSchema.computeDatasetStatistics();
		
		
	    this.vectorOutputFormat = conf.get( CanovaUtils.OUTPUT_VECTOR_FORMAT );
		if (null == vectorOutputFormat) {
			this.vectorOutputFormat = "svmlight";
		}
		
		System.out.println( "Using output vector format: " + this.vectorOutputFormat );
		
    	
    	
    }
    
    protected void cleanup(Context context) {
    	
    	System.out.println( " > Map Task > cleanup() > " );
    	
    	this.csvSchema.debugPrintDatasetStatistics();
    	
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
     * Based on:
     * 
     * https://github.com/deeplearning4j/Canova/blob/master/canova-cli/src/main/java/org/canova/cli/vectorization/CSVVectorizationEngine.java
     * 
     * 
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	// 1. figure out which schema the record is from
    	
    	
    	
    	//System.out.println("Key: " + key.toString() + " Map() > " + value.toString() ); 
    	if (key.get() == 0 && this.skipHeader) {
    		
    		// skip header!
    		System.out.println( "Skipping header: " + value.toString() );
    		
    	} else {
    		
	    	String csvLine = value.toString();
	    	String[] columns = csvLine.split( this.csvSchema.delimiter );
	
	        outputValue.set( SVMLightUtils.generateAeolipileSVMLight(csvSchema, csvLine) ); 
	        
	    	
	    	//System.out.println( "row: " + csvLine );

	        //System.out.println( "SVMLight: " + outputValue.toString() );
	        
	        context.write( NullWritable.get(), outputValue );
	    	
    	}
      
        
        	
        	
  //      	
    	
    }
    
}
