package org.canova.hadoop.mapreduce.vectorization.collectstatistics.csv;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.canova.hadoop.csv.schema.CSVInputSchema;
import org.canova.hadoop.csv.schema.CSVSchemaColumn;
import org.canova.hadoop.utils.CanovaUtils;
import org.canova.hadoop.utils.ConfTools;

//import com.pattersonconsultingtn.ranger.phalanx.conf.ConfTools;

public class CollectStatisticsReduceTask extends Reducer<Text, Text, NullWritable, Text> {
	
	private String outputBasePath = "schema/stats/";
	private String outputErrorPath = "schema/errors/";
	
	private Text result = new Text();
	private MultipleOutputs<NullWritable, Text> multipleOutputWriter;
	
	CSVInputSchema csvSchema = null;
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {

		
		
		Configuration conf = context.getConfiguration();
		System.out.println( "Reduce::setup() method -----" );

    	String wfConfVal = conf.get("oozie.action.id" );
    	if ( null != wfConfVal ) {
    		    		
    		System.out.println( "We're using Oozie" );

    		// INPUT_VECTOR_SCHEMA_FILENAME_KEY
    		String schemaPath = conf.get( CanovaUtils.INPUT_VECTOR_SCHEMA_FILENAME_KEY );
    		if (null == schemaPath) {
    			System.err.println( "No valid input vector schema!" );
    			return;
    		}
    		
    		System.out.println( "schema path: " + schemaPath );
    		
    		try {
    			
    			// oozie hack
    			ConfTools.loadTextFileContentsIntoConf( conf, CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY, schemaPath );
    			System.out.println( schemaPath + " schema file contents loaded into conf..." );
    			
    		} catch (Exception e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}    		
    		
    		
    	}		
		
		// setup multiple output format
		
		this.multipleOutputWriter = new MultipleOutputs<>(context);
		
		// load the schema
		
		String contents = conf.get( CanovaUtils.CONF_VECTOR_SCHEMA_CONTENTS_KEY );
		//System.out.println( "CSV Schema Contents: " + contents );
	
		this.csvSchema = new CSVInputSchema();
		try {
			this.csvSchema.parseSchemaFromRawText(contents);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		super.setup(context);
		
		
    }
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String columnName = key.toString().trim();
		
	//	System.out.println( "col name: " + columnName );
		
		CSVSchemaColumn colSchemaEntry = this.csvSchema.getColumnSchemaByName( columnName );
		
		
		for (Text value : values) {
			
			//System.out.println("value: " + value.toString() );
			
		    String columnValue = value.toString().replaceAll("\"", "");
		    
		    try {
				colSchemaEntry.evaluateColumnValue( columnValue );
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("bad value: " + columnValue );
			}

		    
		    
		}
		
		if (colSchemaEntry.columnType == CSVSchemaColumn.ColumnType.NUMERIC) {

			
			
			// write out min value
			
		//	context.write(new Text( CSVCollectStatisticsMapReduceJob.COLUMN_NOMINAL_MIN_KEY ), new Text(Double.toString( colSchemaEntry.minValue)) );
			
			Text output_min = new Text( CollectStatisticsMapReduceJob.COLUMN_NOMINAL_MIN_KEY + "." + columnName + "=" + Double.toString( colSchemaEntry.minValue ) );
			
			System.out.println( "Writing: " + output_min.toString() );
			
			//this.multipleOutputWriter.write("ColumnStats", NullWritable.get(), output_min , outputBasePath + columnName + ".txt" );
			this.multipleOutputWriter.write( NullWritable.get(), output_min , outputBasePath + columnName + ".txt" );
		
			// write out max value

//			context.write(new Text( CSVCollectStatisticsMapReduceJob.COLUMN_NOMINAL_MAX_KEY ), new Text(Double.toString( colSchemaEntry.maxValue)) );
			
			Text output_max = new Text( CollectStatisticsMapReduceJob.COLUMN_NOMINAL_MAX_KEY + "." + columnName + "=" + Double.toString( colSchemaEntry.maxValue ) );
			
			//this.multipleOutputWriter.write("ColumnStats", NullWritable.get(), output_max, "column_statistics/" + columnName + "_stats");
			this.multipleOutputWriter.write("ColumnStats", NullWritable.get(), output_max, outputBasePath + columnName + ".txt" );

			Text output_sum = new Text( CollectStatisticsMapReduceJob.COLUMN_NOMINAL_SUM_KEY + "." + columnName + "=" + Double.toString( colSchemaEntry.sum ) );
			this.multipleOutputWriter.write("ColumnStats", NullWritable.get(), output_sum, outputBasePath + columnName + ".txt" );
			
			Text output_count = new Text( CollectStatisticsMapReduceJob.COLUMN_NOMINAL_COUNT_KEY + "." + columnName + "=" + Integer.toString( colSchemaEntry.count ) );
			this.multipleOutputWriter.write("ColumnStats", NullWritable.get(), output_count, outputBasePath + columnName + ".txt" );

			
		} else if (colSchemaEntry.columnType == CSVSchemaColumn.ColumnType.NOMINAL) {
		
			Text output_label = new Text();
			
			// write out labels 
			
			for (Map.Entry<String, Pair<Integer,Integer>> entry : colSchemaEntry.recordLabels.entrySet()) {
			    
				String labelKey = entry.getKey();
				Pair<Integer,Integer> labelValue = entry.getValue();
			    
			    //System.out.println( "> " + key + ", " + value);
				 //= new Text( CSVCollectStatisticsMapReduceJob.COLUMN_NOMINAL_MAX_KEY + "=" + Double.toString( colSchemaEntry.maxValue ) );
				
				output_label.set("" + labelValue.getFirst() + "," + labelKey.toString() + "," + labelValue.getSecond() );
				
				if (colSchemaEntry.transform == CSVSchemaColumn.TransformType.LABEL) {
				
					//this.multipleOutputWriter.write("Labels", NullWritable.get(), output_label, "labels/" + columnName + "_labels");
					this.multipleOutputWriter.write("Labels", NullWritable.get(), output_label, outputBasePath + columnName + ".txt" );
				
				} else if (colSchemaEntry.transform == CSVSchemaColumn.TransformType.COPY) {
					
					// has to be COPY
					this.multipleOutputWriter.write("Labels", NullWritable.get(), output_label, outputBasePath + columnName + ".txt" );

				} else if (colSchemaEntry.transform == CSVSchemaColumn.TransformType.NORMALIZE) {
					
					// has to be COPY --- unless we want to normalize label IDs
					this.multipleOutputWriter.write("Labels", NullWritable.get(), output_label, outputBasePath + columnName + ".txt" );
					
					
				} else {
					
					// huh
					System.err.println("Reducer: writing labels, but not !COPY nor !LABEL: " + colSchemaEntry.transform + " for column: " + columnName );
					
				}
			    
			    // now work with key and value...
			}	
			
//			Text output_count = new Text( CSVCollectStatisticsMapReduceJob.COLUMN_NOMINAL_COUNT_KEY + "." + columnName + "=" + Integer.toString( colSchemaEntry.count ) );
//			this.multipleOutputWriter.write("ColumnStats", NullWritable.get(), output_count, outputBasePath + columnName + ".txt" );
			
			
			
//			this.multipleOutputWriter.write(namedOutput, key, value, baseOutputPath)
			
		} else {
			
			Text output_error = new Text();
			
			output_error.set( "Error in column configuration" );
				
			//this.multipleOutputWriter.write("Error", NullWritable.get(), output_error, columnName + "errors/" + columnName + "_errors");
			this.multipleOutputWriter.write("Error", NullWritable.get(), output_error, outputErrorPath + columnName + ".txt");
			    
			
		}
		
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		this.multipleOutputWriter.close();
    }	
}
