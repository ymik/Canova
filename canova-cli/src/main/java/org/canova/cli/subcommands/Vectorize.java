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

package org.canova.cli.subcommands;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

import com.google.common.base.Strings;
import org.canova.api.conf.Configuration;
import org.canova.api.exceptions.CanovaException;
import org.canova.api.formats.input.InputFormat;
import org.canova.api.formats.output.OutputFormat;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;
import org.canova.cli.csv.schema.CSVInputSchema;
import org.canova.cli.csv.vectorization.CSVVectorizationEngine;
import org.canova.cli.vectorization.VectorizationEngine;
import org.canova.nd4j.nlp.vectorizer.TfidfVectorizer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * Vectorize Command.
 * Based on an input and output format
 * transforms data
 *
 * @author jp
 * @author Adam Gibson
 */
public class Vectorize implements SubCommand {

    private static final Logger log = LoggerFactory.getLogger(Vectorize.class);

    public static final String OUTPUT_FILENAME_KEY = "output.directory";
    public static final String INPUT_FORMAT = "input.format";
    public static final String DEFAULT_INPUT_FORMAT_CLASSNAME = "org.canova.api.formats.input.impl.LineInputFormat";
    public static final String OUTPUT_FORMAT = "output.format";
    public static final String DEFAULT_OUTPUT_FORMAT_CLASSNAME = "org.canova.api.formats.output.impl.SVMLightOutputFormat";

    public static final String VECTORIZATION_ENGINE = "input.vectorization.engine";
    public static final String DEFAULT_VECTORIZATION_ENGINE_CLASSNAME = "org.canova.cli.csv.vectorization.CSVVectorizationEngine";

    protected String[] args;

    public boolean validCommandLineParameters = true;

    @Option(name = "-conf", usage = "Sets a configuration file to drive the vectorization process")
    public String configurationFile = "";

    public Properties configProps = null;
    public String outputVectorFilename = "";

//    private CSVInputSchema inputSchema = null;
//    private CSVVectorizationEngine vectorizer = null;

    public Vectorize() {

    }
/*
    // this picks up the input schema file from the properties file and loads it
    private void loadInputSchemaFile() throws Exception {
        String schemaFilePath = (String) this.configProps.get("input.vector.schema");
        this.inputSchema = new CSVInputSchema();
        this.inputSchema.parseSchemaFile(schemaFilePath);
        this.vectorizer = new CSVVectorizationEngine();
    }
*/
    // picked up in the command line parser flags (-conf=<foo.txt>)
    public void loadConfigFile() throws IOException {

        this.configProps = new Properties();

        //Properties prop = new Properties();
        try (InputStream in = new FileInputStream(this.configurationFile)) {
            this.configProps.load(in);
        }

        if (null == this.configProps.get(OUTPUT_FILENAME_KEY)) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            this.outputVectorFilename = "/tmp/canova_vectors_" + dateFormat.format(new Date()) + ".txt";
        } else {

            // what if its only a directory?

            this.outputVectorFilename = (String) this.configProps.get(OUTPUT_FILENAME_KEY);

            if (!(new File(this.outputVectorFilename).exists())) {

                // file path does not exist

                File yourFile = new File(this.outputVectorFilename);
                
                //File targetFile = new File("foo/bar/phleem.css");
                File parent = yourFile.getParentFile();
                if(!parent.exists() && !parent.mkdirs()){
                    throw new IllegalStateException("Couldn't create dir: " + parent);
                }                
                
                if (!yourFile.exists()) {
                    yourFile.createNewFile();
                }

            } else {
                if (new File(this.outputVectorFilename).isDirectory()) {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
                    //File file = new File(dateFormat.format(date) + ".tsv") ;
                    this.outputVectorFilename += "/canova_vectors_" + dateFormat.format(new Date()) + ".txt";
                } else {
                    // if a file already exists
                    (new File(this.outputVectorFilename)).delete();
                    log.info("File path already exists, deleting the old file before proceeding...");
                }
            }
        }
    }

    public void debugLoadedConfProperties() {
        Properties props = this.configProps; //System.getProperties();
        Enumeration e = props.propertyNames();

        log.info("\n--- Start Canova Configuration ---");

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            System.out.println(key + " -- " + props.getProperty(key));
        }

        log.info("---End Canova Configuration ---\n");
    }


    // 1. load conf file
    // 2, load schema file
    // 3. transform csv -> output format
    public void execute() throws Exception  {

    	System.out.println( "Vectorize > execute() [ START ]");
    	
        if (!this.validCommandLineParameters) {
            log.error("Vectorize function is not configured properly, stopping.");
            return;
        }

        //boolean schemaLoaded;
        // load stuff (conf, schema) --> CSVInputSchema

        this.loadConfigFile();

        if (null != this.configProps.get("conf.print")) {
            String print = (String) this.configProps.get("conf.print");
            if ("true".equals(print.trim().toLowerCase())) {
                this.debugLoadedConfProperties();
            }
        }



        // collect dataset statistics --> CSVInputSchema

        // [ first dataset pass ]
        // for each row in CSV Dataset

        String datasetInputPath = (String) this.configProps.get("input.directory");
        String inputDataType = (String)this.configProps.get("input.data.type");
        
        if ( null == inputDataType ) {
        	
        	// yeah cant do this, kick out
        	throw new IllegalStateException("Can't operate without input.data.type being set in the configuration file.");
        	
        }

        // ###########
        // THIS is where we end general vectorization and hit pipeline specific stuff
        // ###########
        Configuration conf = new Configuration();
        conf.set( OutputFormat.OUTPUT_PATH, this.outputVectorFilename );

        
        File inputFile = new File(datasetInputPath);
        InputSplit split = new FileSplit(inputFile);
        InputFormat inputFormat = this.createInputFormat();
        
        
        
        System.out.println( "input file: " + datasetInputPath );

        RecordReader reader = inputFormat.createReader(split, conf);


        VectorizationEngine engine = this.createVectorizationEngine();
        engine.initialize(split, inputFormat, this.createOutputFormat(), reader, this.createOutputFormat().createWriter(conf), this.configProps, this.outputVectorFilename, conf );
        
        engine.execute();
        
        System.out.println( "Vectorize > execute() [ END ]");
        
        log.info("Output vectors written to: " + this.outputVectorFilename);
    }


    /**
     * @param args arguments for command
     */
    public Vectorize(String[] args) {

        this.args = args;
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            this.validCommandLineParameters = false;
            parser.printUsage(System.err);
            log.error("Unable to parse args", e);
        }

    }


    /**
     * Creates an input format
     *
     * @return
     */
    public InputFormat createInputFormat() {

        //System.out.println( "> Loading Input Format: " + (String) this.configProps.get( INPUT_FORMAT ) );

        String clazz = (String) this.configProps.get(INPUT_FORMAT);

        if (null == clazz) {
            clazz = DEFAULT_INPUT_FORMAT_CLASSNAME;
        }

        try {
            Class<? extends InputFormat> inputFormatClazz = (Class<? extends InputFormat>) Class.forName(clazz);
            return inputFormatClazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public OutputFormat createOutputFormat() {
        //String clazz = conf.get( OUTPUT_FORMAT, DEFAULT_OUTPUT_FORMAT_CLASSNAME );
        //System.out.println( "> Loading Output Format: " + (String) this.configProps.get( OUTPUT_FORMAT ) );
        String clazz = (String) this.configProps.get(OUTPUT_FORMAT);
        if (null == clazz) {
            clazz = DEFAULT_OUTPUT_FORMAT_CLASSNAME;
        }

        try {
            Class<? extends OutputFormat> outputFormatClazz = (Class<? extends OutputFormat>) Class.forName(clazz);
            return outputFormatClazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    
    

    /**
     * Creates an input format
     *
     * @return
     */
    public VectorizationEngine createVectorizationEngine() {

    	String clazz = DEFAULT_VECTORIZATION_ENGINE_CLASSNAME; //(String) this.configProps.get( VECTORIZATION_ENGINE );
    	
    	// so this quick lookup is not the coolest way to do this, but for now we'll do it
    	
    	String inputDataType = (String)this.configProps.get("input.data.type");
    	
    	if ("csv".equals(inputDataType)) {

    		clazz = "org.canova.cli.csv.vectorization.CSVVectorizationEngine";

    	} else if ("text".equals(inputDataType)) {

    		clazz = "org.canova.cli.vectorization.TextVectorizationEngine";
    		
    	} else if ("audio".equals(inputDataType)) {

    		clazz = "org.canova.cli.vectorization.AudioVectorizationEngine";

    	} else if ("image".equals(inputDataType)) {

    		clazz = "org.canova.cli.vectorization.ImageVectorizationEngine";

    	} else if ("video".equals(inputDataType)) {

    		clazz = "org.canova.cli.vectorization.VideoVectorizationEngine";

    	} else {
    		// stick to default --- should blow up (?)
    	}
    	
        
/*
        if (null == clazz) {
            clazz = DEFAULT_VECTORIZATION_ENGINE_CLASSNAME;
        }
*/
    	
    	log.debug("Running the " + clazz + " vectorization engine.");
    	
        try {
            Class<? extends VectorizationEngine> vecEngineClazz = (Class<? extends VectorizationEngine>) Class.forName(clazz);
            return vecEngineClazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }    
    
    

}