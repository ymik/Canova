Canova
======

# Description

A tool focused simply on vectorizing raw data into usable vector formats across machine learning tools.

# Example

 * Convert the CSV-based UCI Iris dataset into svmLight open vector text format
 * Convert the MNIST dataset from raw binary files to the svmLight text format.
 * Convert raw text into the Metronome vector format
 * Convert raw text into TF-IDF based vectors in a text vector format {svmLight, metronome, arff}
 * Convert raw text into the word2vec in a text vector format {svmLight, metronome, arff}

# Targeted Vectorization Engines

 * Any CSV to vectors with a scriptable transform language
 * MNIST to vectors
 * Text to vectors
    * TF-IDF
    * Bag of Words
    * word2vec

# CSV Transformation Engine

Below is an example of the CSV transform language in action from the command line

## UCI Iris Schema Transform

```
@RELATION UCIIrisDataset
@DELIMITER ,
 
   @ATTRIBUTE sepallength  NUMERIC   !NORMALIZE
   @ATTRIBUTE sepalwidth   NUMERIC   !NORMALIZE
   @ATTRIBUTE petallength  NUMERIC   !NORMALIZE
   @ATTRIBUTE petalwidth   NUMERIC   !NORMALIZE
   @ATTRIBUTE class        STRING   !LABEL
```

## Setting Up Canova

We need to do a git pull from the github repo and then build the dependencies

```
mvn -DskipTests=true -Dmaven.javadoc.skip=true install
```

Then we'd want to build the stand alone Canova jar to run the CLI from the command line:

```
cd canova-cli/
mvn -DskipTests=true -Dmaven.javadoc.skip=true package
```

## Setup the Configuration File

We need a file to tell the vectorization engine what to do. Create a text file that look like:

```

input.header.skip=false
input.statistics.debug.print=false
input.format=org.canova.api.formats.input.impl.LineInputFormat

input.directory=src/test/resources/csv/data/uci_iris_sample.txt
input.vector.schema=src/test/resources/csv/schemas/uci/iris.txt
output.directory=/tmp/iris_unit_test_sample.txt

output.format=org.canova.api.formats.output.impl.SVMLightOutputFormat

```

## Running Canova From the Command Line

We can now convert UCI's Iris dataset into svmLight from the command line:

```
./bin/canova vectorize -conf [my_conf_file]
```

The output should look like:

```
./bin/canova vectorize -conf /tmp/iris_conf.txt 
File path already exists, deleting the old file before proceeding...
Output vectors written to: /tmp/iris_svmlight.txt

```

# Execution

Runs as both a local serial process and a MapReduce (MR engine on the roadmap) scale out process with no code changes.

# Targetted Vector Formats
* svmLight
* libsvm
* Metronome
* ARFF

# Built-In General Functionality
* Understands how to take general text and convert it into vectors with stock techniques such as kernel hashing and TF-IDF [TODO]
