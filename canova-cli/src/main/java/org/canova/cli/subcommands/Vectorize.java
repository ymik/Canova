package org.canova.cli.subcommands;

import java.util.Properties;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Vectorize implements SubCommand {
	  private static final Logger log = LoggerFactory.getLogger(Vectorize.class);
	  protected String[] args;
	  private String configurationFile = "";
	private Properties configProps = null;


	// this picks up the input schema file from the properties file and loads it
	private void loadInputSchemaFile() {


	}



	// picked up in the command line parser flags (-conf=<foo.txt>)
	private void loadConfigFile() {


	}

	// 1. load conf file
	// 2, load schema file
	// 3. transform csv -> output format
	private void executeVectorizeWorkflow() {

		
		// load stuff (conf, schema) --> CSVInputSchema
		
		// collect dataset statistics --> CSVInputSchema
		
			// [ first dataset pass ]
		
		// generate dataset report --> DatasetSummaryStatistics
		
		// produce converted/vectorized output based on statistics --> Transforms + CSVInputSchema + Rows

			// [ second dataset pass ]		
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
