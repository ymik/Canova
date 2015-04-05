package org.canova.cli.csv.schema;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestCSVInputSchema {

	@Test
	public void testLoadAndValidateSchema() throws Exception {

		String schemaFilePath = "src/test/resources/csv/schemas/unit_test_schema.txt";
		CSVInputSchema inputSchema = new CSVInputSchema();
		inputSchema.parseSchemaFile( schemaFilePath );
		
		inputSchema.debugPrintColumns();
		
		assertEquals( ",", inputSchema.delimiter );
		assertEquals( "SytheticDatasetUnitTest", inputSchema.relation );
		
		assertEquals( CSVSchemaColumn.ColumnType.NUMERIC, inputSchema.getColumnSchemaByName( "sepallength" ).columnType );
		assertEquals( CSVSchemaColumn.TransformType.COPY, inputSchema.getColumnSchemaByName( "sepallength" ).transform );
		
		assertEquals( null, inputSchema.getColumnSchemaByName("foo") );

		assertEquals( CSVSchemaColumn.ColumnType.NUMERIC, inputSchema.getColumnSchemaByName( "class" ).columnType );
		assertEquals( CSVSchemaColumn.TransformType.LABEL, inputSchema.getColumnSchemaByName( "class" ).transform );
		
		
	}

	@Test
	public void testEvaluateCSVRecords() {
		//fail("Not yet implemented");
	}
	
	
	
}
