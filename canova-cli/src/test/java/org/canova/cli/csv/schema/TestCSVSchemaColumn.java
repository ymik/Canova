package org.canova.cli.csv.schema;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestCSVSchemaColumn {

	@Test
	public void testClassBalanceReporting() throws Exception {

		CSVSchemaColumn schemaCol_0 = new CSVSchemaColumn( "a", CSVSchemaColumn.ColumnType.STRING, CSVSchemaColumn.TransformType.LABEL );
	
		schemaCol_0.evaluateColumnValue("alpha");
		schemaCol_0.evaluateColumnValue("beta");
		schemaCol_0.evaluateColumnValue("gamma");

		schemaCol_0.evaluateColumnValue("alpha");
		schemaCol_0.evaluateColumnValue("beta");
		schemaCol_0.evaluateColumnValue("gamma");

		schemaCol_0.evaluateColumnValue("alpha");
		schemaCol_0.evaluateColumnValue("beta");

		schemaCol_0.evaluateColumnValue("alpha");
		
		schemaCol_0.debugPrintColumns();
		
		assertEquals( 3, schemaCol_0.getLabelCount("beta"), 0.0 );
		assertEquals( 2, schemaCol_0.getLabelCount("gamma"), 0.0 );
		assertEquals( 4, schemaCol_0.getLabelCount("alpha"), 0.0 );

	}

	@Test
	public void testMinMaxMetrics() throws Exception {
		
		CSVSchemaColumn schemaCol_0 = new CSVSchemaColumn( "a", CSVSchemaColumn.ColumnType.NUMERIC, CSVSchemaColumn.TransformType.COPY );
		
		schemaCol_0.evaluateColumnValue("1");
		schemaCol_0.evaluateColumnValue("2");
		schemaCol_0.evaluateColumnValue("3");
		
		//schemaCol_0.computeStatistics();
		
		assertEquals( 1, schemaCol_0.minValue, 0.0 );
		assertEquals( 3, schemaCol_0.maxValue, 0.0 );
	}

	@Test
	public void testMinMaxMetricsMixedInsertOrder() throws Exception {
		
		CSVSchemaColumn schemaCol_0 = new CSVSchemaColumn( "a", CSVSchemaColumn.ColumnType.NUMERIC, CSVSchemaColumn.TransformType.COPY );
		
		schemaCol_0.evaluateColumnValue("6");
		schemaCol_0.evaluateColumnValue("-2");
		schemaCol_0.evaluateColumnValue("3");
		
		//schemaCol_0.computeStatistics();
		
		assertEquals( -2, schemaCol_0.minValue, 0.0 );
		assertEquals( 6, schemaCol_0.maxValue, 0.0 );
	}
	
	@Test
	public void testSchemaEvaluation() {
		
		// colSchemaEntry.evaluateColumnValue( columns[ colIndex ] );
		
	}
	
	
	
	
}
