package org.canova.cli.csv.transforms;

/*

	purpose: represent column transforms of CSV data to vectorize

*/
public class Transforms {

	public double copy(String inputColumnValue) {
		throw new UnsupportedOperationException();
	}

	/*
	 * Needed Statistics for binarize()
	 * - range of values (min, max)
	 * - similar to normalize, but we threshold on 0.5 after normalize
	 */
        public double binarize(String inputColumnValue) {
        	throw new UnsupportedOperationException();
        }

    	/*
    	 * Needed Statistics for normalize()
    	 * - range of values (min, max)
    	 * - 
    	 */        
        public double normalize(String inputColumnValue) {
        	throw new UnsupportedOperationException();
        }

    	/*
    	 * Needed Statistics for label()
    	 * - count of distinct labels
    	 * - index of labels to IDs (hashtable?)
    	 */                
        public double label(String inputColumnValue) {
        	throw new UnsupportedOperationException();
        }

}
