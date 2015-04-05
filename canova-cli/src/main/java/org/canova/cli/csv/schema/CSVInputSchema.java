package org.canova.cli.csv.schema;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/*

	purpose: to parse and represent the input schema + column transforms of CSV data to vectorize

*/
public class CSVInputSchema {

	public String relation = "";
	public String delimiter = "";
	private boolean hasComputedStats = false;

	// columns: { columnName, column Schema }
	private Map<String, CSVSchemaColumn> columnSchemas = new LinkedHashMap<String, CSVSchemaColumn>();

	public CSVSchemaColumn getColumnSchemaByName( String colName ) {
		
		return this.columnSchemas.get(colName);
		
	}
	
	private boolean validateRelationLine( String[] lineParts ) {
		
		if (lineParts.length != 2) {
			return false;
		}
		
		return true;
		
	}

	private boolean validateDelimiterLine( String[] lineParts ) {
		
		System.out.println( "delimiter line count: " + lineParts.length);
		
		if (lineParts.length != 2) {
			return false;
		}
		
		return true;
		
	}
	
	private boolean validateAttributeLine( String[] lineParts ) {
		
		if (lineParts.length != 4) {
			return false;
		}
		
		return true;
		
	}
	
	
	
	private boolean validateSchemaLine( String line ) {
		
		//String[] parts = line.trim().split(" ");
		
		System.out.println( line );
		
		
		String lineCondensed = line.trim().replaceAll(" +", " ");
		String[] parts = lineCondensed.split(" ");
		
		if ( parts[ 0 ].toLowerCase().equals("@relation") ) {
			
			return this.validateRelationLine(parts);
			
		} else if ( parts[ 0 ].toLowerCase().equals("@delimiter") ) {
			
			return this.validateDelimiterLine(parts);
			
		} else if ( parts[ 0 ].toLowerCase().equals("@attribute") ) {
			
			return this.validateAttributeLine(parts);
			
		} else if ( parts[ 0 ].trim().equals("") ) {
			
			System.out.println( "Skipping blank line" );
			return true;
			
		} else {
			
			// bad schema line
			System.err.println("Line attribute matched no known attribute in schema! --- " + line);
			return false;
			
		}
		
		
		//return true;
		
	}
	
	private String parseRelationInformation(String[] parts) {
		
		return parts[1];
		
	}
	
	private String parseDelimiter(String[] parts) {
		
		return parts[1];
		
	}
	
	/**
	 * parse out lines like:
	 * 		@ATTRIBUTE sepallength  NUMERIC   !COPY
	 * 
	 * @param parts
	 * @return
	 */
	private CSVSchemaColumn parseColumnSchemaFromAttribute( String[] parts ) {
		
		String columnName = parts[1];
		String columnType = parts[2];
		String columnTransform = parts[3];
		
		CSVSchemaColumn.ColumnType colTypeEnum = null;
		CSVSchemaColumn.TransformType colTransformEnum = null;
		
		if ( "NUMERIC".equals( columnType.toUpperCase() ) ) {
			colTypeEnum = CSVSchemaColumn.ColumnType.NUMERIC;
		} else if ( "DATE".equals( columnType.toUpperCase() ) ) {
			colTypeEnum = CSVSchemaColumn.ColumnType.DATE;
		} else if ( "NOMINAL".equals( columnType.toUpperCase() ) ) {
			colTypeEnum = CSVSchemaColumn.ColumnType.NOMINAL;
		} else if ( "STRING".equals( columnType.toUpperCase() ) ) {
			colTypeEnum = CSVSchemaColumn.ColumnType.STRING;
		}
		
		if ( "!COPY".equals( columnTransform.toUpperCase() ) ) {
			colTransformEnum = CSVSchemaColumn.TransformType.COPY;
		} else if ( "!BINARIZE".equals( columnTransform.toUpperCase() ) ) {
			colTransformEnum = CSVSchemaColumn.TransformType.BINARIZE;
		} else if ( "!LABEL".equals( columnTransform.toUpperCase() ) ) {
			colTransformEnum = CSVSchemaColumn.TransformType.LABEL;
		} else if ( "!NORMALIZE".equals( columnTransform.toUpperCase() ) ) {
			colTransformEnum = CSVSchemaColumn.TransformType.NORMALIZE;
		} else if ( "!SKIP".equals( columnTransform.toUpperCase() ) ) {
			colTransformEnum = CSVSchemaColumn.TransformType.SKIP;
			
		}
		
		CSVSchemaColumn colValue = new CSVSchemaColumn( columnName, colTypeEnum, colTransformEnum );
		
		return colValue;
		
	}
	
	private void addSchemaLine( String line ) {
		
		// parse out: columnName, columnType, columnTransform
		String lineCondensed = line.trim().replaceAll(" +", " ");
		String[] parts = lineCondensed.split(" ");
		
		if ( parts[ 0 ].toLowerCase().equals("@relation") ) {
			
		//	return this.validateRelationLine(parts);
			this.relation = parts[1];
			
		} else if ( parts[ 0 ].toLowerCase().equals("@delimiter") ) {
			
		//	return this.validateDelimiterLine(parts);
			this.delimiter = parts[1];
			
		} else if ( parts[ 0 ].toLowerCase().equals("@attribute") ) {
			
			String key = parts[1];
			CSVSchemaColumn colValue = this.parseColumnSchemaFromAttribute( parts );
			
			this.columnSchemas.put( key, colValue );
			
		} else {
			
			
		}		
		
		
	}
	
	public void parseSchemaFile(String schemaPath) throws Exception {

		//throw new UnsupportedOperationException();
		
		try (BufferedReader br = new BufferedReader( new FileReader( schemaPath ) )) {
			
		    for (String line; (line = br.readLine()) != null; ) {
		        // process the line.
		    	if (false == this.validateSchemaLine(line) ) {
		    		throw new Exception("Bad Schema for CSV Data");
		    	}
		    	
		    	// now add it to the schema cache
		    	this.addSchemaLine(line);
		    	
		    }
		    // line is not visible here.
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void evaluateInputRecord(String csvRecordLine) throws Exception {
		
		// does the record have the same number of columns that our schema expects?
		
		String[] columns = csvRecordLine.split( this.delimiter );
		
		if (columns.length != this.columnSchemas.size() ) {
			
			throw new Exception("Row column count does not match schema column count.");
			
		}
		
		int colIndex = 0;
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {
		
			
			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();
		    
		    // now work with key and value...
		    colSchemaEntry.evaluateColumnValue( columns[ colIndex ] );
		    
		    colIndex++;
		    
		}		
		
		
		
	}
	
	/**
	 * We call this method once we've scanned the entire dataset once to gather column stats
	 * 
	 */
	public void computeDatasetStatistics() {
		
		
		
		this.hasComputedStats = true;
		
	}
	
	public void debugPrintColumns() {
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {
		    
			String key = entry.getKey();
		    CSVSchemaColumn value = entry.getValue();
		    
		    // now work with key and value...
		    
		    System.out.println( "> " + value.name + ", " + value.columnType + ", " + value.transform );
		    
		}		
		
	}
	


}
