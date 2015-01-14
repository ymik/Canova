package org.canova.nlp.vectorizer;

import org.nd4j.api.berkeley.Counter;
import org.nd4j.api.conf.Configuration;
import org.nd4j.api.records.reader.RecordReader;
import org.nd4j.api.vector.Vectorizer;
import org.nd4j.api.writable.Writable;
import org.nd4j.nlp.metadata.DefaultVocabCache;
import org.nd4j.nlp.metadata.VocabCache;
import org.nd4j.nlp.stopwords.StopWords;
import org.nd4j.nlp.tokenization.tokenizer.Tokenizer;
import org.nd4j.nlp.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Baseline text vectorizer that includes some common elements
 * to text analysis such as the tokenizer factory
 *
 * @author Adam Gibson
 */
public abstract class TextVectorizer<VECTOR_TYPE> implements Vectorizer<VECTOR_TYPE> {

    protected TokenizerFactory tokenizerFactory;
    protected int minWordFrequency = 0;
    public final static String MIN_WORD_FREQUENCY = "org.nd4j.nlp.minwordfrequency";
    public final static String STOP_WORDS = "org.nd4j.nlp.stopwords";
    protected Collection<String> stopWords;
    protected VocabCache cache = new DefaultVocabCache();

    @Override
    public void initialize(Configuration conf) {
        tokenizerFactory = createTokenizerFactory(conf);
        minWordFrequency = conf.getInt(MIN_WORD_FREQUENCY,5);
        stopWords = conf.getStringCollection(STOP_WORDS);
        if(stopWords == null || stopWords.isEmpty())
            stopWords = StopWords.getStopWords();
        cache = new DefaultVocabCache();
    }

    @Override
    public void fit(RecordReader reader) {
        fit(reader,null);
    }

    @Override
    public void fit(RecordReader reader, RecordCallBack callBack) {
        while(reader.hasNext()) {
            Collection<Writable> record = reader.next();
            String s = toString(record);
            Tokenizer tokenizer = tokenizerFactory.create(s);
            cache.incrementNumDocs(1);
            doWithTokens(tokenizer);
            if(callBack != null)
                callBack.onRecord(record);


        }
    }


    protected Counter<String> wordFrequenciesForRecord(Collection<Writable> record) {
        String s = toString(record);
        Tokenizer tokenizer = tokenizerFactory.create(s);
        Counter<String> ret = new Counter<>();
        while(tokenizer.hasMoreTokens())
            ret.incrementCount(tokenizer.nextToken(),1.0);
        return ret;
    }


    protected String toString(Collection<Writable> record) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        for(Writable w : record) {
            try {
                w.write(dos);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String s = new String(bos.toByteArray());
        return s;
    }


    /**
     * Increment counts, add to collection,...
     * @param tokenizer
     */
    public abstract void doWithTokens(Tokenizer tokenizer);

    /**
     * Create tokenizer factory based on the configuration
     * @param conf the configuration to use
     * @return the tokenizer factory based on the configuration
     */
    public abstract TokenizerFactory createTokenizerFactory(Configuration conf);

}
