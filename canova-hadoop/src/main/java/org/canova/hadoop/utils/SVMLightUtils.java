package org.canova.hadoop.utils;

import java.util.Map;

//import org.apache.spark.mllib.linalg.DenseVector;
//import org.apache.spark.mllib.linalg.Vectors;
import org.canova.hadoop.csv.schema.CSVInputSchema;
import org.canova.hadoop.csv.schema.CSVSchemaColumn;


public class SVMLightUtils {
	
	/**
	 * Not sure in what cases we use this over the modified one below for Aeolipile case
	 * 
	 * @param schema
	 * @param csvLine
	 * @return
	 */
/*	public static String generateRegularSVMLight( CSVInputSchema schema, String csvLine ) {
		
		String outputLine = "";
		StringBuilder outBuilder = new StringBuilder("");
		
    	//String csvLine = value.toString();
    	String[] columns = csvLine.split( schema.delimiter );
		
    	if (columns[0].trim().equals("")) {

    		
    	} else {
    		
    		int srcColIndex = 0;
		    int dstColIndex = 1;

		    //log.info( "> Engine.vectorize() ----- ");

		    double label = 0;    		
    	
		    double valueTmp = 0;
	
	    	// FOR EACH KEY in the column set
	        // scan through the columns in the schema / input csv data
	        for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
	
	          CSVSchemaColumn schemaColumnEntry = entry.getValue();
	          	          
	          if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.SKIP) {
	        	  
	        	  srcColIndex++;
	        	  
	          } else if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.LABEL) {
	        	  
	        	  label = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          } else {
	        	  valueTmp = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
	        	  if (valueTmp > 0.0) {
	        		  outBuilder.append( " " + dstColIndex + ":" + valueTmp );
	        	  }
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          }
	          
	          
	        }
	        
	        // SVMLight format: <label> <index-1>:<value> 2:<value> (...) N:<value>
	        outputLine = label + outBuilder.toString(); 
	        
	        
    	}    	
		
		return outputLine;
		
	}
*/
	public static String generateAeolipileSVMLight( CSVInputSchema schema, String csvLine ) {
		
		String outputLine = "";
		StringBuilder outBuilder = new StringBuilder("");
		
    	//String csvLine = value.toString();
    	String[] columns = csvLine.split( schema.delimiter );
		
    	if (columns[0].trim().equals("")) {

    		
    	} else {
    		
    		int srcColIndex = 0;
		    int dstColIndex = 1;

		    //log.info( "> Engine.vectorize() ----- ");

		    double label = 0;    
		    String accountID = "";
    	
		    double valueTmp = 0;
	
	    	// FOR EACH KEY in the column set
	        // scan through the columns in the schema / input csv data
	        for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
	
	          CSVSchemaColumn schemaColumnEntry = entry.getValue();
	          	          
	          if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.SKIP) {
	        	  
	        	  srcColIndex++;

	          } else if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.UNIQUE_ID) {
	        	  
	        	  //accountID = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] ) + "";
	        	  accountID = columns[ srcColIndex ];
		          srcColIndex++;
		          
	        	  
	          } else if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.LABEL) {
	        	  
	        	  label = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          } else {
	        	  valueTmp = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
	        	  
	        	  // TODO: potential huge bug --- negative values dont show up!!!
	        	  // if (valueTmp > 0.0) {
	        	  // FIXED, but leaving this note for future Josh to re-read and go "yeah dont fuck w that" 
	        	  if (valueTmp != 0.0) {
	        		  outBuilder.append( " " + dstColIndex + ":" + valueTmp );
	        	  }
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          }
	          
	          
	        }
	        
	        // SVMLight format: <label> <index-1>:<value> 2:<value> (...) N:<value>
	        if ("".equals( accountID.trim() )) {
	        	outputLine = label + outBuilder.toString();
	        } else {
	        	outputLine = accountID + " " + label + outBuilder.toString(); 
	        }
	        
	        
    	}    	
		
		return outputLine;		
	}
	
	public static String getSVMLightRecordFromAeolipileRecord( String aeolipileRecord ) {
    	
    	String work = aeolipileRecord.trim();
    	int firstSpaceIndex = work.indexOf(' ');
    	String newRecord = work.substring(firstSpaceIndex, work.length());
    	
    	return newRecord.trim();

		
	}

	public static String getUniqueIDFromAeolipileRecord( String aeolipileRecord ) {
    	
    	String work = aeolipileRecord.trim();
    	int firstSpaceIndex = work.indexOf(' ');
    	String newRecord = work.substring( 0, firstSpaceIndex );
    	
    	return newRecord.trim();

		
	}
	
	public static String getLabel(String SVMLightRecord) {
		
    	String work = SVMLightRecord.trim();
    	int firstSpaceIndex = work.indexOf(' ');
    	String label = work.substring( 0, firstSpaceIndex );
    	
    	return label.trim();
		
		
	}
/*	
	public static DenseVector convertSVMLightTo_Dense_Vector(String rawLine, int size) {
		
		//Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
		
		//System.out.println( "line: " + rawLine );
		
		String[] parts = rawLine.trim().split(" ");
		//System.out.println( "part count: " + parts.length);
		//int[] indicies = new int[ size ]; //[ parts.length - 1 ];
		double[] values = new double[ size ]; //[ parts.length - 1 ];
		
		// skip the label
		//for ( int x = 1; x <  parts.length; x++ ) {
		
		//System.out.println( "Label: " + parts[ 0 ] );
		
		int currentPartsIndex = 1;
		
		for ( int x = 1; x < size + 1; x++ ) {
			
			
			
			String[] indexValueParts = parts[ currentPartsIndex ].split(":");
			int parsedIndex = Integer.parseInt( indexValueParts[ 0 ] );
			if (x == parsedIndex) {

				values[ x - 1 ] = Double.parseDouble(indexValueParts[ 1 ]);

				currentPartsIndex++;
				
			} else {

				values[ x - 1 ] = 0; //Double.parseDouble(indexValueParts[ 1 ]);

			}
			
			//System.out.println( " x = " + x + " -> " + values[ x - 1 ] + ", "+ parts[ x ]);
			
			
		}
		
		// Vectors.dense(1.0, 0.0, 3.0)
		//return new SparseVector(size, indicies, values);
		return (DenseVector) Vectors.dense( values );
		
	}	
	
	*/
	
}
