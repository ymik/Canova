/*
 *
 *  *
 *  *  * Copyright 2015 Skymind,Inc.
 *  *  *
 *  *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *  *    you may not use this file except in compliance with the License.
 *  *  *    You may obtain a copy of the License at
 *  *  *
 *  *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  *    Unless required by applicable law or agreed to in writing, software
 *  *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  *    See the License for the specific language governing permissions and
 *  *  *    limitations under the License.
 *  *
 *
 */

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
			vecCommand.executeVectorizeWorkflow();


		} else {
			
			System.out.println( "Canova's command line system only supports the 'vectorize' command." );
			
		}
		
		
		
		
	}	

}
