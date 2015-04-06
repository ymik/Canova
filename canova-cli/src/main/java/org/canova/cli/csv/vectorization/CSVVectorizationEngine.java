package org.canova.cli.csv.vectorization;

import java.util.Map;

import org.canova.cli.csv.schema.CSVInputSchema;
import org.canova.cli.csv.schema.CSVSchemaColumn;
import org.canova.cli.csv.schema.CSVSchemaColumn.TransformType;

/**
 * Vectorization Engine
 * - takes CSV input and converts it to a transformed vector output in a standard format
 * - uses the input CSV schema and the collected statistics from a pre-pass
 * 
 * @author josh
 *
 */
public class CSVVectorizationEngine {

	/**
	 * Use statistics collected from a previous pass to vectorize (or drop) each column
	 * 
	 * @return
	 */
	public String vectorize( String key, String value, CSVInputSchema schema ) {
		
		// TODO: this needs to be different (needs to be real vector representation
		String outputVector = "";
		String[] columns = value.split( schema.delimiter );
		
		int colIndex = 0;
		
		for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
		
			
			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();
		    
		    // produce the per column transform based on stats
		    
		    if ( TransformType.SKIP == colSchemaEntry.transform ) {
		    	
		    	// dont append this to the output vector, skipping
		    	
		    } else {
		    
		    	double convertedColumn = colSchemaEntry.transformColumnValue( columns[ colIndex ] );
		    	// add this value to the output vector

		    }
		    
		    
		    
		    colIndex++;
		    
		}		
		
		
		return null;
		
	}
	
	
}
