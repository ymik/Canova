package org.canova.cli.driver;

import java.io.IOException;
import java.util.Arrays;

import org.canova.api.exceptions.CanovaException;
import org.canova.cli.subcommands.Vectorize;
import org.kohsuke.args4j.Option;

public class CommandLineInterfaceDriver {
		
	
	public static void main(String [ ] args) {
	    /*
		System.out.println( "CommandLineInterfaceDriver > Printing args:");
		
		for ( String arg : args ) {
			
			System.out.println( ">> " + arg );
			
		}
		*/
		
		if ("vectorize".equals( args[ 0 ] )) {

			String[] vec_params = Arrays.copyOfRange(args, 1, args.length);
			
			Vectorize vecCommand = new Vectorize( vec_params );
			try {
				vecCommand.execute();
			} catch (CanovaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		} else {
			
			System.out.println( "Canova's command line system only supports the 'vectorize' command." );
			
		}
		
		
		
		
	}	

}
