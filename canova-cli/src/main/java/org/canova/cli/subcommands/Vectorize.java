package org.canova.cli.subcommands;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.canova.cli.csv.schema.CSVInputSchema;
import org.canova.cli.csv.vectorization.CSVVectorizationEngine;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Vectorize implements SubCommand {
	private static final Logger log = LoggerFactory.getLogger(Vectorize.class);
	protected String[] args;
	public String configurationFile = "";
	public Properties configProps = null;

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
	public void loadConfigFile() {

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


	}


	// 1. load conf file
	// 2, load schema file
	// 3. transform csv -> output format
	public void executeVectorizeWorkflow() {

		boolean schemaLoaded = false;
		// load stuff (conf, schema) --> CSVInputSchema

		this.loadConfigFile();

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

		// TODO: replace this with an { input-format, record-reader }
		try (BufferedReader br = new BufferedReader( new FileReader( datasetInputPath ) )) {

			for (String line; (line = br.readLine()) != null; ) {

				// TODO: this will end up processing key-value pairs

				// this outputVector needs to be ND4J
				String outputVector = String.valueOf(this.vectorizer.vectorize( "", line, this.inputSchema ));

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
}
