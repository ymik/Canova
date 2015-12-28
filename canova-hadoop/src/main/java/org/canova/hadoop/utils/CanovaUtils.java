package org.canova.hadoop.utils;

public class CanovaUtils {
	
	// "canova.input.directory"

	public static final String INPUT_DATA_FILENAME_KEY = "canova.input.directory";
	
    //public static final String OUTPUT_DATA_FILENAME_COLLECT_STATS_KEY = "canova.output.directory.statistics";
    //public static final String OUTPUT_DATA_FILENAME_DERIVE_STATS_KEY = "canova.output.directory.statistics.derived";
    public static final String OUTPUT_DATA_FILENAME_VECTORIZATION_KEY = "canova.output.directory.vectorization";
    
	
	public static final String INPUT_VECTOR_SCHEMA_FILENAME_KEY = "canova.input.vector.schema";
	public static final String CONF_VECTOR_SCHEMA_CONTENTS_KEY = "canova.input.vector.schema";
    
    
    //public static final String INPUT_FORMAT = "canova.input.format";
    //public static final String DEFAULT_INPUT_FORMAT_CLASSNAME = "org.canova.api.formats.input.impl.LineInputFormat";
    //public static final String OUTPUT_FORMAT = "canova.output.format";
    //public static final String DEFAULT_OUTPUT_FORMAT_CLASSNAME = "org.canova.api.formats.output.impl.SVMLightOutputFormat";

    //public static final String VECTORIZATION_ENGINE = "canova.input.vectorization.engine";
    //public static final String DEFAULT_VECTORIZATION_ENGINE_CLASSNAME = "org.canova.cli.csv.vectorization.CSVVectorizationEngine";

    public static final String NORMALIZE_DATA_FLAG = "canova.input.vectorization.normalize";
    public static final String SHUFFLE_DATA_FLAG = "canova.output.shuffle";
    public static final String PRINT_STATS_FLAG = "canova.input.statistics.debug.print";
	
    public static final String OUTPUT_VECTOR_FORMAT = "canova.output.vector.format";
	
}
