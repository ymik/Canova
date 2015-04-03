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
	public double minValue = 0;
	public double maxValue = 0;
	public double stddev = 0;
	public double median = 0;
	
	// used to track input values that do not match the schema data type
	public long invalidDataEntries = 0;
	
	

	// we want to track the label counts to understand the class balance
	private Map<String, Integer> recordLabels = new LinkedHashMap<String, Integer>();
	
	/**
	 * This method collects dataset statistics about the column that we'll 
	 * need later to
	 * 1. convert the column based on the requested transforms
	 * 2. report on column specfic statistics to give visibility into the properties of the input dataset
	 * 
	 * @param value
	 */
	public void evaluateColumnValue(String value) {
		
		if ( ColumnType.NUMERIC == this.columnType ) {
			
			
		} else if ( ColumnType.STRING == this.columnType ) {
			
			
		}
		
	}
	

}

