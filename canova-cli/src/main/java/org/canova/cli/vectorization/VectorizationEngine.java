package org.canova.cli.vectorization;

import java.io.IOException;
import java.util.Properties;

import org.canova.api.conf.Configuration;
import org.canova.api.exceptions.CanovaException;
import org.canova.api.formats.input.InputFormat;
import org.canova.api.formats.output.OutputFormat;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.split.InputSplit;

public abstract class VectorizationEngine {
	
	protected InputFormat inputFormat = null;
	protected OutputFormat outputFormat = null;
	protected InputSplit split = null;
	protected RecordWriter writer = null;
	protected RecordReader reader = null;
	protected Properties configProps = null;
	protected String outputFilename = null;
	protected Configuration conf = null;
	
	public void initialize( InputSplit split, InputFormat inputFormat, OutputFormat outputFormat, RecordReader reader, RecordWriter writer, Properties configProps, String outputFilename, Configuration conf) {
		
		this.split = split;
		this.reader = reader;
		this.writer = writer;
		this.configProps = configProps;
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
		this.outputFilename = outputFilename;
		this.conf = conf;
		
	}
	
	public abstract void execute() throws CanovaException, IOException, InterruptedException;
	
	/**
	 * These two methods are stubbing the future vector transform transform system
	 * 
	 * We want to separate the transform logic from the inputformat/recordreader 
	 * 	-	example: a "thresholding" function that binarizes the vector entries
	 * 	-	example: a sampling function that takes a larger images and down-samples the image into a small vector
	 * 
	 */
	public void addTransform() {
		throw new UnsupportedOperationException();
	}
	
	public void applyTransforms() {
		throw new UnsupportedOperationException();
	}
	

}
