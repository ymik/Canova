package org.canova.image.mnist.draw;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;


public class DrawMnist {
	public static void drawMnist(DataSet mnist,INDArray reconstruct) throws InterruptedException {
		for(int j = 0; j < mnist.numExamples(); j++) {
			INDArray draw1 = mnist.get(j).getFeatureMatrix().mul(255);
			INDArray reconstructed2 = reconstruct.getRow(j);
			INDArray draw2 = Nd4j.getDistributions().createBinomial(1,reconstructed2).sample(reconstructed2.shape()).mul(255);

			DrawReconstruction d = new DrawReconstruction(draw1);
			d.title = "REAL";
			d.draw();
			DrawReconstruction d2 = new DrawReconstruction(draw2,1000,1000);
			d2.title = "TEST";
			
			d2.draw();
			Thread.sleep(1000);
			d.frame.dispose();
			d2.frame.dispose();

		}
	}

}
