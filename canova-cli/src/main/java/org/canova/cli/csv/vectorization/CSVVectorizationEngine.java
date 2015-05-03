package org.canova.cli.csv.vectorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.canova.cli.csv.schema.CSVInputSchema;
import org.canova.cli.csv.schema.CSVSchemaColumn;
import org.canova.cli.csv.schema.CSVSchemaColumn.TransformType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vectorization Engine
 * - takes CSV input and converts it to a transformed vector output in a standard format
 * - uses the input CSV schema and the collected statistics from a pre-pass
 * 
 * @author josh
 *
 */
public class CSVVectorizationEngine {

    private static Logger log = LoggerFactory.getLogger(CSVVectorizationEngine.class);
    
	/**
	 * Use statistics collected from a previous pass to vectorize (or drop) each column
	 * 
	 * @return
	 */
	public Collection<Writable> vectorize( String key, String value, CSVInputSchema schema ) {
		
		//INDArray
        Collection<Writable> ret =  new ArrayList<>();
		
		// TODO: this needs to be different (needs to be real vector representation
		//String outputVector = "";
		String[] columns = value.split( schema.delimiter );
				
		if ( columns[0].trim().equals("") ) {
		//	log.info("Skipping blank line");
			return null;
		}
		
		int srcColIndex = 0;
		int dstColIndex = 0;
		
		//log.info( "> Engine.vectorize() ----- ");
		
		double label = 0;
		
		// scan through the columns in the schema / input csv data
		for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
			
			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();
		    
		    // produce the per column transform based on stats
		    
		    if ( TransformType.SKIP == colSchemaEntry.transform ) {
		    	
		    	// dont append this to the output vector, skipping
		    } else if ( TransformType.LABEL == colSchemaEntry.transform ) {
		    	
		    //	log.info( " label value: " + columns[ srcColIndex ] );
		    	
		    	label = colSchemaEntry.transformColumnValue( columns[ srcColIndex ].trim() );
		    	
		    } else {
		    
		    //	log.info( " column value: " + columns[ srcColIndex ] );
		    	
		    	double convertedColumn = colSchemaEntry.transformColumnValue( columns[ srcColIndex ].trim() );
		    	// add this value to the output vector
                ret.add(new DoubleWritable(convertedColumn));

		    	dstColIndex++;
		    	
		    }
		    
		    
		    
		    srcColIndex++;
		    
		}		
		
        ret.add(new DoubleWritable(label));

    	//dstColIndex++;
		
		
		return ret;
		
	}
	
	/**
	 * Use statistics collected from a previous pass to vectorize (or drop) each column
	 * 
	 * @return
	 */
	public Collection<Writable> vectorizeToWritable( String key, String value, CSVInputSchema schema ) {
		
		//INDArray
		//INDArray ret = this.createArray( schema.getTransformedVectorSize() );
		Collection<Writable> ret = new ArrayList<>();
		
		// TODO: this needs to be different (needs to be real vector representation
		//String outputVector = "";
		String[] columns = value.split( schema.delimiter );
				
		if ( columns[0].trim().equals("") ) {
		//	log.info("Skipping blank line");
			return null;
		}
		
		int srcColIndex = 0;
		int dstColIndex = 0;
		
		//log.info( "> Engine.vectorize() ----- ");
		
		// scan through the columns in the schema / input csv data
		for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
			
			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();
		    
		    // produce the per column transform based on stats
		    
		    if ( TransformType.SKIP == colSchemaEntry.transform ) {
		    	
		    	// dont append this to the output vector, skipping
		    	
		    } else {
		    
		    //	log.info( " column value: " + columns[ srcColIndex ] );
		    	
		    	double convertedColumn = colSchemaEntry.transformColumnValue( columns[ srcColIndex ].trim() );
		    	// add this value to the output vector

		    	//ret.putScalar(dstColIndex, convertedColumn);
		    	ret.add(new Text(convertedColumn + ""));
		    	
		    	dstColIndex++;
		    	
		    }
		    
		    
		    
		    srcColIndex++;
		    
		}		
		
		
		return ret;
		
	}	
	

	
	
	
}
