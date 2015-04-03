package org.canova.cli.csv.schema;

/*

	purpose: to parse and represent the input schema + column transforms of CSV data to vectorize

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
	
	// TODO: hashmap(?) of labels
	// may wnat to track the label counts to understand the class balance
	
	public void addLabel(String label) {
		
		
	}

}

