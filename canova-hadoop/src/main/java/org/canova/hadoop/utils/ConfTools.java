package org.canova.hadoop.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.canova.hadoop.csv.schema.CSVInputSchema;


public class ConfTools {
	
	
	
    
    /**
     * We save the number of schemas so we know the suffix for the base key
     * to pull the contexts from the conf file
     * 
     * if the schema files get too big we'll move them to DCache
     * 
     * @param conf
     * @param paths
     * @throws Exception 
     */
/*    public static void loadAllSchemaFilesIntoConf( Configuration conf, String[] paths ) throws Exception {
    	
    //	int schemaCount = paths.length;
    	
    //	conf.set( SCHEMA_COUNT, schemaCount + "" );
    	
    	for (String path : paths) {
    		
    		loadInputSchemaFileContentsIntoConf( conf, path );
    		
    	}
    	
    }
    */
/*    
    public static Map<String, Schema> loadAllSchemaFilesFromConfAsSchemas( Configuration conf, String[] paths ) throws Exception {

    	Map<String, Schema> schemaNameToSchemaMap = new LinkedHashMap<String, Schema>();
    	
    	for (String path : paths) {
    		
    		//loadInputSchemaFileContentsIntoConf( conf, path );
    		String schemaContentsFromConf = conf.get(path);
    		Schema inputSchema = new Schema();
    		inputSchema.parseSchemaFromRawText( schemaContentsFromConf );
    		schemaNameToSchemaMap.put( inputSchema.relation, inputSchema );
    		
    	}
    	
    	return schemaNameToSchemaMap;
    	
    } 
    */   
    
    /**
     * TODO: determine how we are gonna ref the mapping of schema file->input dir
     * 
     * 
     * @param conf
     * @param schemaFilePath
     * @param mappedInputPath
     * @throws Exception
     */
/*	public static void loadInputSchemaFileContentsIntoConf( Configuration conf, String schemaFilePath ) throws Exception {

		// 1. load the contexts of the schema files
		
		Schema inputSchema = new Schema();
		inputSchema.parseSchemaFile( schemaFilePath );

		
		conf.set( schemaFilePath, inputSchema.rawTextSchema );
		
	}
	
	public static Schema getSchemaFromConf( Configuration conf, String confKey ) throws Exception {

		Schema confSchema = new Schema();

		String rawSchema = conf.get(confKey);
		
		confSchema.parseSchemaFromRawText(rawSchema);
		
		return confSchema;
		
	}
*/


	// picked up in the command line parser flags (-conf=<foo.txt>)
	public static void loadConfigFileContentsIntoConf( Configuration conf, String confFilePath ) throws IOException {

		Properties configProps = new Properties();
		
		//Properties prop = new Properties();
		InputStream in = null;
		try {
			in = new FileInputStream( confFilePath );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			configProps.load(in);
			in.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			

		// for each directory->schema mapping load the file contents into the conf object

	//	Map<String, Schema> mappings = SchemaConfTools.parseSchemaMappings(conf, confEntry, entryDelimiter, pairDelimiter)
	
		Enumeration e = configProps.propertyNames();
	    while (e.hasMoreElements()) {
		      String key = (String) e.nextElement();
		      //System.out.println(key + " -- " + configProps.getProperty(key));
		      conf.set( key, configProps.getProperty(key) );
	    }		
		
		

	}
	
	
	
	// picked up in the command line parser flags (-conf=<foo.txt>)
	public static void loadConfigFileContentsFromHDFSIntoConf( Configuration conf, String confFilePath ) throws IOException {

		System.out.println( "Using new hdfs-enabled read path..." );
		
		FileSystem fs = FileSystem.get(conf);
		
		Properties configProps = new Properties();
		
		//Properties prop = new Properties();
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path(confFilePath) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			configProps.load(in);
			in.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			

		// for each directory->schema mapping load the file contents into the conf object

	//	Map<String, Schema> mappings = SchemaConfTools.parseSchemaMappings(conf, confEntry, entryDelimiter, pairDelimiter)
	
		Enumeration e = configProps.propertyNames();
	    while (e.hasMoreElements()) {
		      String key = (String) e.nextElement();
		      //System.out.println(key + " -- " + configProps.getProperty(key));
		      conf.set( key, configProps.getProperty(key) );
	    }		
		
		

	}	
	
	
	
	public static void loadTextFileContentsIntoConf( Configuration conf, String confKey, String filename ) throws IOException {

		StringBuilder b = new StringBuilder();
		
		FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path(filename) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		//try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
		    for (String line; (line = br.readLine()) != null; ) {

		    	b.append(line + "\n");
		    	
		    }
		    // line is not visible here.
		}
		
		
		
		conf.set( confKey, b.toString() );
		in.close();

	}		
	

	public static String loadTextFileContentsAsString( Configuration conf, String filename ) throws IOException {

		StringBuilder b = new StringBuilder();
		
		FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path(filename) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}

		
		//try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
		    for (String line; (line = br.readLine()) != null; ) {

		    	b.append(line + "\n");
		    	
		    }
		    // line is not visible here.
		}
		
		
		
		//conf.set( confKey, b.toString() );
		in.close();

		return b.toString();
	}		
	
/*
	public static void loadSchemaContentsIntoConf( Configuration conf, String confKey, String filename ) throws IOException {

		// 1. load the contexts of the schema files
		
		Schema inputSchema = new Schema();
		try {
			inputSchema.parseSchemaFile( filename );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		conf.set( confKey, inputSchema.rawTextSchema );
		

	}	
*/
	
/*
	public static void loadSchemaContentsFromHDFSIntoConf( Configuration conf, String confKey, String filename ) throws IOException {

		// 1. load the contexts of the schema files
		
		Schema inputSchema = new Schema();
		
		FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path(filename) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		StringBuilder b = new StringBuilder();
		
		try  {

			BufferedReader br = new BufferedReader( new InputStreamReader(in) );
			
		    for (String line; (line = br.readLine()) != null; ) {

		    	b.append( line + "\n" );
		    	
		    }
		    // line is not visible here.
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		
		conf.set( confKey, b.toString() );
		

	}	
	*/
	

	public static void loadVectorSchemaContentsIntoConf( Configuration conf, String confKey, String filename ) throws Exception {

		// 1. load the contexts of the schema files
		
		CSVInputSchema inputSchema = new CSVInputSchema();

		inputSchema.parseSchemaFile( filename );
		// if that doesnt kick out, then just take the raw string and put it in the 

		String confFileContents = "";
		//InputStream in = null;
		try {
			//in = new FileInputStream( filename );
			//confFileContents = in.
			
			confFileContents = new String(Files.readAllBytes(Paths.get(filename)));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//in.close();
		
		conf.set( confKey, confFileContents );
		

	}		
	
	public static void debugLoadedConfProperties( Configuration conf ) {
		/*
		Properties props = this.configProps; //System.getProperties();
	    Enumeration e = props.propertyNames();

	    System.out.println("\n--- Canova Configuration ---");
	    
	    while (e.hasMoreElements()) {
	      String key = (String) e.nextElement();
	      System.out.println(key + " -- " + props.getProperty(key));
	    }		
		
	    System.out.println("--- Canova Configuration ---\n");
	    */
		
		for (Map.Entry<String, String> entry : conf) {
            System.out.println(entry.getKey() + " => " + entry.getValue());
        }		
		
	} 	

}
