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
