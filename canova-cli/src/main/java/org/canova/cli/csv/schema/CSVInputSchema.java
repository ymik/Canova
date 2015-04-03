package org.canova.cli.csv.schema;

import java.util.LinkedHashMap;
import java.util.Map;

/*

	purpose: to parse and represent the input schema + column transforms of CSV data to vectorize

*/
public class CSVInputSchema {

	public String relation = "";
	public String delimiter = "";

	// columns: { columnName, column Schema }
	private Map<String, CSVSchemaColumn> columnSchemas = new LinkedHashMap<String, CSVSchemaColumn>();

	public void parseSchemaFile() {

		throw new UnsupportedOperationException();

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
	
	public void debugPrintColumns() {
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {
		    
			String key = entry.getKey();
		    CSVSchemaColumn value = entry.getValue();
		    
		    // now work with key and value...
		}		
		
	}
	


}
