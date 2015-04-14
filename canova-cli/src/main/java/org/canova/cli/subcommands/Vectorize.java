package org.canova.cli.subcommands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

//import org.canova.api.records.reader.RecordReader;

import org.canova.api.conf.Configuration;
import org.canova.api.exceptions.CanovaException;
import org.canova.api.formats.input.InputFormat;
import org.canova.api.formats.input.impl.LineInputFormat;
import org.canova.api.formats.output.OutputFormat;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.records.writer.impl.SVMLightRecordWriter;
import org.canova.cli.csv.schema.CSVInputSchema;
import org.canova.cli.csv.vectorization.CSVVectorizationEngine;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Vectorize implements SubCommand {
	  
	private static final Logger log = LoggerFactory.getLogger(Vectorize.class);

	
	public static final String OUTPUT_FILENAME_KEY = "output.directory";
	
	  public static final String INPUT_FORMAT = "input.format";
	  public static final String DEFAULT_INPUT_FORMAT_CLASSNAME = "org.canova.api.formats.input.impl.LineInputFormat";
	  public static final String OUTPUT_FORMAT = "output.format";
	  public static final String DEFAULT_OUTPUT_FORMAT_CLASSNAME = "org.canova.api.formats.output.impl.SVMLightOutputFormat";
	  
	  protected String[] args;
	  public String configurationFile = "";
	public Properties configProps = null;
	public String outputVectorFilename = "";
	
	private CSVInputSchema inputSchema = null; //
	private CSVVectorizationEngine vectorizer = null;

	
	public Vectorize() {
		
		
	}

	// this picks up the input schema file from the properties file and loads it
	private void loadInputSchemaFile() throws Exception {

		String schemaFilePath = (String) this.configProps.get("input.vector.schema");
		this.inputSchema = new CSVInputSchema();
		this.inputSchema.parseSchemaFile( schemaFilePath );

		this.vectorizer = new CSVVectorizationEngine();
	}



	// picked up in the command line parser flags (-conf=<foo.txt>)
	public void loadConfigFile() throws IOException {

		this.configProps = new Properties();
		
		//Properties prop = new Properties();
		InputStream in = null;
		try {
			in = new FileInputStream( this.configurationFile );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.configProps.load(in);
			in.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		
		/*

			new File(path).exists();
			
			
			This will tell you if it is a directory:
			
			new File(path).isDirectory();
			
			
			Similarly, this will tell you if it's a file:
			
			new File(path).isFile();

		 */
		
		if (null == this.configProps.get( OUTPUT_FILENAME_KEY )) {
			
			Date date = new Date() ;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss") ;
			//File file = new File(dateFormat.format(date) + ".tsv") ;
			
			this.outputVectorFilename = "/tmp/SVMLight_" + dateFormat.format(date) + ".txt";
			
			System.out.println( "No output file specified, defaulting to: " + this.outputVectorFilename );
			
		} else {
			
			// what if its only a directory?
			
			this.outputVectorFilename = (String) this.configProps.get( OUTPUT_FILENAME_KEY );
			
			if ( (new File( this.outputVectorFilename ).exists()) == false ) {
				
				// file path does not exist
				
				File yourFile = new File( this.outputVectorFilename );
				if(!yourFile.exists()) {
				    yourFile.createNewFile();
				} 
				
			} else {
				
				if ( new File( this.outputVectorFilename ).isDirectory() ) {
					
					Date date = new Date() ;
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss") ;
					//File file = new File(dateFormat.format(date) + ".tsv") ;
					
					this.outputVectorFilename += "/SVMLight_" + dateFormat.format(date) + ".txt";
					
					
				} else {
					
					// if a file that exists
					
					System.out.println( "File path already exists, using default" );
					
					Date date = new Date() ;
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss") ;
					//File file = new File(dateFormat.format(date) + ".tsv") ;
					
					this.outputVectorFilename += "/SVMLight_" + dateFormat.format(date) + ".txt";

					
				}
				
				
			}
			
			System.out.println( "Writing vectorized output to: " + this.outputVectorFilename + "\n\n" );
			
		}
		

	}
	
	public void debugLoadedConfProperties() {
		
		Properties props = this.configProps; //System.getProperties();
	    Enumeration e = props.propertyNames();

	    while (e.hasMoreElements()) {
	      String key = (String) e.nextElement();
	      System.out.println(key + " -- " + props.getProperty(key));
	    }		
		
	}
	

	// 1. load conf file
	// 2, load schema file
	// 3. transform csv -> output format
	public void executeVectorizeWorkflow() throws CanovaException, IOException {

		boolean schemaLoaded = false;
		// load stuff (conf, schema) --> CSVInputSchema
		
		this.loadConfigFile();
		
		this.debugLoadedConfProperties();
		
		
		try {
			this.loadInputSchemaFile();
			schemaLoaded = true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			schemaLoaded = false;
		}
		
		if (false == schemaLoaded) {
		
			// if we did not load the schema then we cannot proceed with conversion
			
		}
		
		// setup input / output formats
		
		
		// collect dataset statistics --> CSVInputSchema
		
			// [ first dataset pass ]
			// for each row in CSV Dataset
		
		String datasetInputPath = (String) this.configProps.get("input.directory");
		
		System.out.println( "Raw Data to convert: " + datasetInputPath );
		
		// TODO: replace this with an { input-format, record-reader }
		try (BufferedReader br = new BufferedReader( new FileReader( datasetInputPath ) )) {
			
		    for (String line; (line = br.readLine()) != null; ) {

		    	// TODO: this will end up processing key-value pairs
		    	this.inputSchema.evaluateInputRecord(line);
		    	
		    }
		    // line is not visible here.
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		// generate dataset report --> DatasetSummaryStatistics
		
		this.inputSchema.computeDatasetStatistics();
		this.inputSchema.debugPringDatasetStatistics();
		
		// produce converted/vectorized output based on statistics --> Transforms + CSVInputSchema + Rows

			// [ second dataset pass ]	
		
		System.out.println( " Second Data Pass > Vectorizing each Column ------" );
		
		OutputFormat outputFormat = this.createOutputFormat();
		
		Configuration conf = new Configuration();
		conf.set(OutputFormat.OUTPUT_PATH, this.outputVectorFilename);
		
        //File tmpOutSVMLightFile = new File("/tmp/vectorsTmp.svmLight");
        RecordWriter writer = outputFormat.createWriter(conf); //new SVMLightRecordWriter(tmpOutSVMLightFile,true);

		
		// TODO: replace this with an { input-format, record-reader }
		try (BufferedReader br = new BufferedReader( new FileReader( datasetInputPath ) )) {
			
		    for (String line; (line = br.readLine()) != null; ) {

		    	// TODO: this will end up processing key-value pairs

		    	// this outputVector needs to be ND4J
		    	// TODO: we need to be re-using objects here for heap churn purposes
		    	//INDArray outputVector = this.vectorizer.vectorize( "", line, this.inputSchema );
		    	if (line.trim().equals("") == false ) {
		    		writer.write( vectorizer.vectorizeToWritable( "", line, this.inputSchema ) );
		    	}
		    	
		    }
		    // line is not visible here.
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}


	  /**
	   * @param args arguments for command
	   */
	  public Vectorize(String[] args) {
	    this.args = args;
	    CmdLineParser parser = new CmdLineParser(this);
	    try {
	      parser.parseArgument(args);
	    } catch (CmdLineException e) {
	      parser.printUsage(System.err);
	      log.error("Unable to parse args", e);
	    }

	  }
	  
	  

    public InputFormat createInputFormat() {
    	
    	System.out.println( "> Loading Input Format: " + (String) this.configProps.get( INPUT_FORMAT ) );
    	
        String clazz = (String) this.configProps.get( INPUT_FORMAT );
        
        if ( null == clazz ) {
        	clazz = DEFAULT_INPUT_FORMAT_CLASSNAME;
        }
        
        try {
            Class<? extends InputFormat> inputFormatClazz = (Class<? extends InputFormat>) Class.forName(clazz);
            return inputFormatClazz.newInstance();
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
        
    }


    public OutputFormat createOutputFormat() {
	    //String clazz = conf.get( OUTPUT_FORMAT, DEFAULT_OUTPUT_FORMAT_CLASSNAME );
    	
    	System.out.println( "> Loading Output Format: " + (String) this.configProps.get( OUTPUT_FORMAT ) );
    	
    	
        String clazz = (String) this.configProps.get( OUTPUT_FORMAT );
        
        if ( null == clazz ) {
        	clazz = DEFAULT_OUTPUT_FORMAT_CLASSNAME;
        }
    	
	    
	    try {
	        Class<? extends OutputFormat> outputFormatClazz = (Class<? extends OutputFormat>) Class.forName(clazz);
	        return outputFormatClazz.newInstance();
	    } catch (Exception e) {
	       throw new RuntimeException(e);
	    }

    }

	  
	  
	  
	  
	  
}
