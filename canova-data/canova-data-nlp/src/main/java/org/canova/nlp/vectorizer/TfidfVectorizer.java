package org.nd4j.nlp.vectorizer;

import org.nd4j.api.conf.Configuration;
import org.nd4j.api.records.reader.RecordReader;
import org.nd4j.api.writable.Writable;
import org.nd4j.nlp.tokenization.tokenizer.Tokenizer;
import org.nd4j.nlp.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.nd4j.nlp.tokenization.tokenizerfactory.TokenizerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * Tf idf vectorizer
 * @author Adam Gibson
 */
public abstract class TfidfVectorizer<VECTOR_TYPE> extends TextVectorizer<VECTOR_TYPE> {




    @Override
    public void doWithTokens(Tokenizer tokenizer) {
        Set<String> seen = new HashSet<>();
        while(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            cache.incrementCount(token);
            if(!seen.contains(token)) {
                cache.incrementDocCount(token);
            }
        }
    }

    @Override
    public TokenizerFactory createTokenizerFactory(Configuration conf) {
        return new DefaultTokenizerFactory();
    }

    @Override
    public abstract VECTOR_TYPE createVector(Object[] args);

    @Override
    public abstract VECTOR_TYPE fitTransform(RecordReader reader);

    @Override
    public abstract VECTOR_TYPE transform(Collection<Writable> record);
}
