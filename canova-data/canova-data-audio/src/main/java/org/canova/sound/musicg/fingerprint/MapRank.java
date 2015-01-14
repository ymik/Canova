package org.nd4j.sound.musicg.fingerprint;

import java.util.List;

public interface MapRank{
	public List getOrderedKeyList(int numKeys, boolean sharpLimit);
}