/*
 *
 *  *
 *  *  * Copyright 2015 Skymind,Inc.
 *  *  *
 *  *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *  *    you may not use this file except in compliance with the License.
 *  *  *    You may obtain a copy of the License at
 *  *  *
 *  *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  *    Unless required by applicable law or agreed to in writing, software
 *  *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  *    See the License for the specific language governing permissions and
 *  *  *    limitations under the License.
 *  *
 *
 */

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
