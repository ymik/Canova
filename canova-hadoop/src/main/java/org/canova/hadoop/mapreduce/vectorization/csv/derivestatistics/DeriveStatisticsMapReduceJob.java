package org.canova.hadoop.mapreduce.vectorization.csv.derivestatistics;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.canova.hadoop.csv.schema.CSVInputSchema;
import org.canova.hadoop.csv.schema.CSVSchemaColumn;
import org.canova.hadoop.mapreduce.vectorization.csv.vectorize.VectorizeMapReduceJob;
import org.canova.hadoop.utils.CanovaUtils;
import org.canova.hadoop.utils.ConfTools;


public class DeriveStatisticsMapReduceJob  extends Configured implements Tool {
	
	public static final String COLUMN_NOMINAL_MIN_KEY = "canova.data.column.statistics.minimum";
	public static final String COLUMN_NOMINAL_MAX_KEY = "canova.data.column.statistics.maximum";
	
	public static final String COLUMN_NOMINAL_SUM_KEY = "canova.data.column.statistics.sum";
	public static final String COLUMN_NOMINAL_COUNT_KEY = "canova.data.column.statistics.count";
	
	public static final String COLUMN_NOMINAL_AVG_KEY = "canova.data.column.statistics.avg";
	public static final String COLUMN_NOMINAL_STD_KEY = "canova.data.column.statistics.std";
	
	public static final String COLUMN_NAME_KEY = "canova.data.column.name";

	public static final String SKIP_HEADER_KEY = "canova.header.skip";
	
	public static final String DERIVED_STATS_OUTPUT_KEY = "canova.input.vector.schema.statistics.derived";


//Configuration conf, 
    public static void setupJob(Job job, String confFile) throws Exception {
    	
    	Configuration conf = job.getConfiguration();
    	FileSystem fs = FileSystem.get(conf);
    	
    	
//    	Configuration conf = job.getConfiguration();
    	
        job.setJarByClass( DeriveStatisticsMapReduceJob.class);
        
        job.setMapperClass( DeriveStatisticsMapTask.class );
        job.setReducerClass( DeriveStatisticsReduceTask.class );

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Text, MapWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        // params to parse from confs
        // 1. master conf file ( join mappings, output directory, output schema )
        // 2. vector schema file 

		try {
			// put each property entry into the configuration file, line by line
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
			ConfTools.loadTextFileContentsIntoConf(conf, CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY, schemaPath);
			System.out.println( schemaPath + " schema file contents loaded into conf..." );
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
		// ------------ Put the schema information files into the DCache -----------
		
		// for each column in the schema, put in dcache
		
		String contents = conf.get( CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY );
		System.out.println( "CSV Schema Contents: \n" + contents );
	
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
	        	  
	        	  System.out.println( "SKIPPING Dcache file: " + vectorSchemaStatsFilePath + " [UNIQUE_ID Column]" );
	        	  
	          } else {
	        	  
	        	  System.out.println( "Adding Dcache file: " + vectorSchemaStatsFilePath );
	          
	        	  job.addCacheFile(new URI( vectorSchemaStatsFilePath ));
	        	  
	          }
	          
        }   		
		
		
		
		
		
		
		
		String inputDataPath = "";
		inputDataPath = conf.get(CanovaUtils.INPUT_DATA_FILENAME_KEY);
		
		if (null == inputDataPath) {
			throw new Exception( "No Input Data!" );
		}
		
		System.out.println( " Adding MR Input Path " + inputDataPath );
        FileInputFormat.addInputPath(job, new Path( inputDataPath ));
		
		// cilantro.output.directory
		
		System.out.println( "Writing output to: " + conf.get( DERIVED_STATS_OUTPUT_KEY ) );
		
        FileOutputFormat.setOutputPath( job, new Path( conf.get( DERIVED_STATS_OUTPUT_KEY ) ) );

		// translate the input paths from the conf file
        
     // Defines additional single text based output 'text' for the job
        //MultipleOutputs.addNamedOutput(job, "Labels", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "ColumnStats", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Error", TextOutputFormat.class, NullWritable.class, Text.class);

        
        
        System.out.println( "Setting up multiple outputs..." );
		
		// get the output directory from the conf file
		
        
		
        System.out.println( "------- Job Setup Complete ------- " );
    	
    }
	
	@Override
	public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: Phalanx_DeriveStatistics_Job <conf_file>");
            System.exit(2);
        }
        
        System.out.println( "remaining flags: " + otherArgs.length );
        String confFile = otherArgs[ 0 ];
        
        Job job = new Job(conf, "Phalanx_DeriveStatistics_Job");
        
        setupJob( job, confFile );
        
        return job.waitForCompletion(true) ? 0 : 1;

	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DeriveStatisticsMapReduceJob(), args);
        System.exit(res);
    }
	
	public static int workflowDriver(String[] args) throws Exception {
		return ToolRunner.run(new Configuration(), new DeriveStatisticsMapReduceJob(), args);
	}	
	
}
