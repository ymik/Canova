package org.canova.cli.subcommands;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseSubCommand  implements SubCommand {
	  private static final Logger log = LoggerFactory.getLogger(BaseSubCommand.class);
	  protected String[] args;

	  /**
	   * @param args arguments for command
	   */
	  public BaseSubCommand(String[] args) {
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
