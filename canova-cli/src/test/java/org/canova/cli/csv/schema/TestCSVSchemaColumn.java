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
		
	
}
