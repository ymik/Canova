package org.canova.cli.csv.schema;

import java.util.LinkedHashMap;
import java.util.Map;

/*

	purpose: to parse and represent the input schema + column transforms of CSV data to vectorize



TODO: 

- produce the following metrics per column

		min
		max
		stddev
		median (percentiles 1, 25, 50, 75, 99)
		range
		outliers
		classify: {Normal, uniform, and skewed} distributions
		render: Histograms and box plots



*/
public class CSVSchemaColumn {
	
	public enum ColumnType { NUMERIC, DATE, NOMINAL, STRING };
	public enum TransformType { COPY, SKIP, BINARIZE, NORMALIZE, LABEL };

	public String name = ""; // the name of the attribute/column
	public ColumnType columnType = null;
	public TransformType transform = null; 

	/*
	 * TODO:
	 * - how do we model statistics per column?
	 * 
	 */
	public double minValue = Double.NaN;
	public double maxValue = Double.NaN;
	//public double stddev = 0;
	//public double median = 0;
	
	// used to track input values that do not match the schema data type
	public long invalidDataEntries = 0;
	
	

	// we want to track the label counts to understand the class balance
	private Map<String, Integer> recordLabels = new LinkedHashMap<String, Integer>();
	
	
	public CSVSchemaColumn(String colName, ColumnType colType, TransformType transformType) {
		
		this.name = colName;
		this.columnType = colType;
		this.transform = transformType;
		
	}
	
	/**
	 * This method collects dataset statistics about the column that we'll 
	 * need later to
	 * 1. convert the column based on the requested transforms
	 * 2. report on column specfic statistics to give visibility into the properties of the input dataset
	 * 
	 * @param value
	 * @throws Exception 
	 */
	public void evaluateColumnValue(String value) throws Exception {
		
		if ( ColumnType.NUMERIC == this.columnType ) {
			
			// then we want to look at min/max values
			
			double tmpVal = Double.parseDouble(value);
			
			System.out.println( "converted: " + tmpVal );
			
			if (Double.isNaN(tmpVal)) {
				throw new Exception("The column was defined as Numeric yet could not be parsed as a Double");
			}
			
			if ( Double.isNaN( this.minValue ) ) {
			
				this.minValue = tmpVal;
				
			} else if (tmpVal < this.minValue) {
				
				this.minValue = tmpVal;
				
			}
			
			if ( Double.isNaN( this.maxValue ) ) {
				
				this.maxValue = tmpVal;
				
			} else if (tmpVal > this.maxValue) {
				
				this.maxValue = tmpVal;
				
			}
			
		} else if ( TransformType.LABEL == this.transform ) {
			
			
			
			// then we want to track the record label
			if ( this.recordLabels.containsKey( value ) ) {
				
				Integer countInt = this.recordLabels.get( value );
				countInt++;
				this.recordLabels.put( value, countInt );
				
			} else {
				
				this.recordLabels.put( value, 1 );
				
			}
			
		}
		
	}
	
	public void computeStatistics() {
		
		if ( ColumnType.NUMERIC == this.columnType ) {
			
		//} else if ( Column == this.columnType ) {
			
		} else {
			
			
		}
		
	}
	
	public void debugPrintColumns() {
		
		for (Map.Entry<String, Integer> entry : this.recordLabels.entrySet()) {
		    
			String key = entry.getKey();
		    Integer value = entry.getValue();
		    
		    System.out.println( "> " + key + ", " + value);
		    
		    // now work with key and value...
		}		
		
	}
	
	public Integer getLabelCount( String label ) {
				
		return this.recordLabels.get( label );
		
	}
	
	

}

