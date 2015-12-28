package org.canova.hadoop.mapreduce.vectorization.collectstatistics.csv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.canova.hadoop.utils.CanovaUtils;
import org.canova.hadoop.utils.ConfTools;


public class CollectStatisticsMapReduceJob extends Configured implements Tool {
	
	public static final String COLUMN_NOMINAL_MIN_KEY = "canova.data.column.statistics.minimum";
	public static final String COLUMN_NOMINAL_MAX_KEY = "canova.data.column.statistics.maximum";
	
	public static final String COLUMN_NOMINAL_SUM_KEY = "canova.data.column.statistics.sum";
	public static final String COLUMN_NOMINAL_COUNT_KEY = "canova.data.column.statistics.count";
	
	public static final String COLUMN_NOMINAL_AVG_KEY = "canova.data.column.statistics.avg";
	
	public static final String COLUMN_NOMINAL_STDDEV_KEY = "canova.data.column.statistics.stddev";
	public static final String COLUMN_NOMINAL_VARIANCE_KEY = "canova.data.column.statistics.variance";
	
	public static final String COLUMN_NAME_KEY = "canova.data.column.name";

	public static final String SKIP_HEADER_KEY = "canova.header.skip";
	
	public static final String OUTPUT_KEY = "canova.input.vector.schema.statistics";


//Configuration conf, 
    public static void setupJob(Job job, String confFile) throws Exception {
    	
    	Configuration conf = job.getConfiguration();
    	
        job.setJarByClass( CollectStatisticsMapReduceJob.class );
        
        job.setMapperClass( CollectStatisticsMapTask.class );
        job.setReducerClass( CollectStatisticsReduceTask.class );

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
			ConfTools.loadTextFileContentsIntoConf( conf, CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY, schemaPath );
			System.out.println( schemaPath + " schema file contents loaded into conf..." );
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		
        
        
		System.out.println( "Writing output to: " + conf.get( OUTPUT_KEY ) );
				//CanovaUtils.OUTPUT_DATA_FILENAME_COLLECT_STATS_KEY ) );
		
        FileOutputFormat.setOutputPath( job, new Path( conf.get( OUTPUT_KEY ) ) ); 
        
        		//conf.get( CanovaUtils.OUTPUT_DATA_FILENAME_COLLECT_STATS_KEY ) ) );

		// translate the input paths from the conf file
        
     // Defines additional single text based output 'text' for the job
        MultipleOutputs.addNamedOutput(job, "Labels", TextOutputFormat.class, NullWritable.class, Text.class);
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
            System.err.println("Usage: Canova_CSVCollectStatistics_Job <conf_file>");
            System.exit(2);
        }
        
        System.out.println( "remaining flags: " + otherArgs.length );
        String confFile = otherArgs[ 0 ];
        
        Job job = new Job(conf, "Canova_CSVCollectStatistics_Job");
        
        setupJob( job, confFile );
        
 //       int code = job.waitForCompletion(true) ? 0 : 1;
   //     System.exit(code);
        
        return job.waitForCompletion(true) ? 0 : 1;

	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CollectStatisticsMapReduceJob(), args);
        System.exit(res);
    }
	
	public static int workflowDriver(String[] args) throws Exception {
		return ToolRunner.run(new Configuration(), new CollectStatisticsMapReduceJob(), args);
	}
	
}
