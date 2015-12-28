package org.canova.hadoop.mapreduce.vectorization.csv.vectorize;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.canova.hadoop.csv.schema.CSVInputSchema;
import org.canova.hadoop.csv.schema.CSVSchemaColumn;
import org.canova.hadoop.mapreduce.vectorization.csv.derivestatistics.DeriveStatisticsMapReduceJob;
import org.canova.hadoop.utils.CanovaUtils;
import org.canova.hadoop.utils.ConfTools;


public class VectorizeMapReduceJob extends Configured implements Tool {
	
	public static final String COLUMN_NOMINAL_MIN_KEY = "canova.data.column.statistics.minimum";
	public static final String COLUMN_NOMINAL_MAX_KEY = "canova.data.column.statistics.maximum";
	public static final String COLUMN_NAME_KEY = "canova.data.column.name";

	public static final String VECTOR_SCHEMA_STATS_INPUT_KEY = "canova.input.vector.schema.statistics";

//Configuration conf, 
    public static void setupJob(Job job, String confFile) throws Exception {
    	
    	Configuration conf = job.getConfiguration();
    	FileSystem fs = FileSystem.get(conf);
    	
        job.setJarByClass( VectorizeMapReduceJob.class );
        
        job.setMapperClass( VectorizeMapTask.class );
        //job.setReducerClass( CSVCollectStatisticsReduceTask.class );

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Text, MapWritable
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(Text.class);
        
        // params to parse from confs
        // 1. master conf file ( join mappings, output directory, output schema )
        // 2. vector schema file 

		try {
			// put each property entry into the configuration file, line by line
			//ConfTools.loadConfigFileContentsIntoConf( conf, confFile );
			ConfTools.loadConfigFileContentsFromHDFSIntoConf( conf, confFile );
		//	System.out.println( "Main Config File Loaded: " + confFile );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// INPUT_VECTOR_SCHEMA_FILENAME_KEY
		String schemaPath = conf.get( CanovaUtils.INPUT_VECTOR_SCHEMA_FILENAME_KEY );
		if (null == schemaPath) {
			System.err.println( "No valid input vector schema!" );
			return;
		}
		
		System.out.println( "schema path: " + schemaPath );
		
//		String[] schemaPaths = SchemaConfTools.parseSchemaFileList( conf.get( MapReduceBuildCanonicalTable.INPUT_SCHEMA_PATHS ), ",", "#" );
//		System.out.println( "Found " + schemaPaths.length + " schema paths in the config file..." );
		
		try {
			
			// loads the contents of the file into the conf
			//ConfTools.loadVectorSchemaContentsIntoConf(conf, CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY, schemaPath);
			ConfTools.loadTextFileContentsIntoConf(conf, CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY, schemaPath);
			System.out.println( schemaPath + " schema file contents loaded into conf..." );
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		// ------------ Put the schema information files into the DCache -----------
		
		// for each column in the schema, put in dcache
		
		String contents = conf.get( CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY );
	//	System.out.println( "CSV Schema Contents: " + contents );
	
		CSVInputSchema csvTempSchema = new CSVInputSchema();
		try {
			csvTempSchema.parseSchemaFromRawText(contents);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
        for (Map.Entry<String, CSVSchemaColumn> entry : csvTempSchema.getColumnSchemas().entrySet()) {
        	
	          String schemaColumnKey = entry.getKey();
	          CSVSchemaColumn schemaColumnEntry = entry.getValue();
	          
	          // stats job's output path
	          
	          //String vectorSchemaStatsFileBasePath = conf.get( VECTOR_SCHEMA_STATS_INPUT_KEY ) + "schema/stats/";

	          //String vectorSchemaStatsFilePath = vectorSchemaStatsFileBasePath + schemaColumnKey.toString() + ".txt-r-00000";

	          
	          String stats_job_output_path = conf.get( VectorizeMapReduceJob.VECTOR_SCHEMA_STATS_INPUT_KEY );
	          if (!stats_job_output_path.endsWith("/")) {
	        	  stats_job_output_path += "/";
	          }
	          
	          String vectorSchemaStatsFileBasePath = stats_job_output_path + "schema/stats/";

	          String vectorSchemaStatsFilePath = vectorSchemaStatsFileBasePath + schemaColumnKey.toString() + ".txt-r-00000";

	          
	          
	          
	          org.apache.hadoop.fs.Path tmpFilePath = new org.apache.hadoop.fs.Path( vectorSchemaStatsFilePath );
	          //tmpFilePath
	          if (fs.exists(tmpFilePath) == false && entry.getValue().transform != CSVSchemaColumn.TransformType.UNIQUE_ID) {
	        	  
	        	  throw new Exception("\t\tSchema Stats File does not exist: " + vectorSchemaStatsFilePath);
	        	
	          } else if (entry.getValue().transform == CSVSchemaColumn.TransformType.UNIQUE_ID) {
	        	  // its the unique id column, no column stats!
	        	  
	        	  System.out.println( "SKIPPING Dcache file (Stats): " + vectorSchemaStatsFilePath + " [UNIQUE_ID Column]" );
	        	  
	          } else {
	        	  
	        	  System.out.println( "Adding Dcache file (Stats): " + vectorSchemaStatsFilePath );
	          
	        	  job.addCacheFile(new URI( vectorSchemaStatsFilePath ));
	        	  
	          }	          
	          
	          // now check for the derived statistics
	          
	          String vectorSchemaDerivedStatsBase = conf.get( DeriveStatisticsMapReduceJob.DERIVED_STATS_OUTPUT_KEY );
	          if (!vectorSchemaDerivedStatsBase.endsWith("/")) {
	        	  vectorSchemaDerivedStatsBase += "/";
	          }
	          
	          String vectorSchemaDerivedStatsFileBasePath = vectorSchemaDerivedStatsBase + "schema/derived_stats/";

	          String vectorSchemaDerivedStatsFilePath = vectorSchemaDerivedStatsFileBasePath + schemaColumnKey.toString() + "_derived" + ".txt-r-00000";

	          
	          
	          org.apache.hadoop.fs.Path tmpDerivedStatsFilePath = new org.apache.hadoop.fs.Path( vectorSchemaDerivedStatsFilePath );
	          //tmpFilePath
	          if (fs.exists(tmpDerivedStatsFilePath) == false && entry.getValue().transform != CSVSchemaColumn.TransformType.UNIQUE_ID) {
	        	  
	        	//  System.out.println( "SKIPPING Dcache file: " + vectorSchemaDerivedStatsFilePath + " [Doesn't Exist]" );
	        	  
	        //	  System.out.println("\t\tSchema Stats File does not exist: " + vectorSchemaDerivedStatsFilePath);
	          } else if (entry.getValue().transform == CSVSchemaColumn.TransformType.UNIQUE_ID) {
	        	  // its the unique id column, no column stats!
	        	  
	        	  System.out.println( "SKIPPING Dcache file (Derived Stats): " + vectorSchemaDerivedStatsFilePath + " [UNIQUE_ID Column]" );
	        	  
	          } else {
	        	  
	        	  //System.out.println( "\nDerived File Base Path: " + vectorSchemaDerivedStatsFileBasePath );
	        	  System.out.println( "Adding Derived Stats File Path (Derived Stats): " + vectorSchemaDerivedStatsFilePath );
	        	  job.addCacheFile(new URI( vectorSchemaDerivedStatsFilePath ));
	        	  
	          }	          
	          
        }        
        
        // DCACHE: http://stackoverflow.com/questions/21239722/hadoop-distributedcache-is-deprecated-what-is-the-preferred-api
        // Mind the # sign after the absolute file location.
        // You will be using the name after the # sign as your
        // file name in your Mapper/Reducer
        //job.addCacheFile(new URI("/user/yourname/cache/some_file.json#some"));
        //job.addCacheFile(new URI("/user/yourname/cache/other_file.json#other"));    
		
		
		
		
		
		
    	/*
		String[] inputFilePaths = SchemaConfTools.parseMapReduceInputFileListFromInputSchemaMappings( conf.get( MapReduceBuildCanonicalTable.INPUT_SCHEMA_PATHS ), ",", "#" );

		for ( int x = 0; x < inputFilePaths.length; x++ ) {
			
			System.out.println( " Adding MR Input Path " + x + ": " + inputFilePaths[ x ] );
			try {
				FileInputFormat.addInputPath( job, new Path( inputFilePaths[ x ] ) );
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		*/
		
		String inputDataPath = "";
		inputDataPath = conf.get(CanovaUtils.INPUT_DATA_FILENAME_KEY);
		
		if (null == inputDataPath) {
			throw new Exception( "No Input Data!" );
		}
		
		System.out.println( " Adding MR Input Path " + inputDataPath );
        FileInputFormat.addInputPath(job, new Path( inputDataPath ));
		
		// cilantro.output.directory
		
		System.out.println( "Writing output to: " + conf.get( CanovaUtils.OUTPUT_DATA_FILENAME_VECTORIZATION_KEY ) );
		
        FileOutputFormat.setOutputPath( job, new Path( conf.get( CanovaUtils.OUTPUT_DATA_FILENAME_VECTORIZATION_KEY ) ) );
		
        
		
        System.out.println( "------- Job Setup Complete ------- " );
    	
    }
	
	@Override
	public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: Canova_CSVCollectStatistics_Job <conf_file>");
            System.exit(2);
        }
        
        System.out.println( "remaining flags: " + otherArgs.length );
        String confFile = otherArgs[ 0 ];
        
        Job job = new Job(conf, "Canova_CSVCollectStatistics_Job");
        
        setupJob( job, confFile );
        return job.waitForCompletion(true) ? 0 : 1;

	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VectorizeMapReduceJob(), args);
        System.exit(res);
    }
	
	public static int workflowDriver(String[] args) throws Exception {
		return ToolRunner.run(new Configuration(), new VectorizeMapReduceJob(), args);
	}	
}
