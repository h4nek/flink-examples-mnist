/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Flink application that creates negatives of the MNIST database images using the DataSet class.
 */
public class NegativeImages {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// create the stream of matrices represented as a DataSet (bounded set of elements)
//        MNISTDataSetIO mnistDataSetIO = new MNISTDataSetIO();
//        AtomicInteger width = new AtomicInteger();
//        AtomicInteger height = new AtomicInteger();
//        DataSet<byte[]>  matrices = env.fromCollection(
//                mnistDataSetIO.readIDX("input/train-images.idx3-ubyte", width, height));
        String imagesPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-images.idx3-ubyte";
        MNISTFileInputFormat mnistHandler = new MNISTFileInputFormat(imagesPath);
		DataSet<byte[]> matrices = env.readFile(mnistHandler, imagesPath);
		
        // transform the images into negatives
        DataSet<byte[]> negativesStream = matrices.map(new NegateImageMap());
        
        // save the negatives in the specified path
//        mnistDataSetIO.saveImages("output/outputNegatives", "negative", negatives, 
//                height.get(), width.get());
        String outputPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\output\\outputNegatives\\";
        negativesStream.output(new PngOutputFormat<>(outputPath, "negative", 
                               mnistHandler.getNumCols(), mnistHandler.getNumRows()));
        
        //matrices.write();
        
		// execute program
		env.execute("Turn MNIST Images into Negatives");
	}
}

class NegateImageMap implements MapFunction<byte[], byte[]> {
    @Override
    public byte[] map(byte[] image) throws Exception {
        for (int i = 0; i < image.length; ++i) {
            byte pixel = image[i];
            image[i] = (byte) ((-1)* (pixel < 0 ? pixel-1 : pixel+1)); // negate each pixel which is in SIGNED byte format
        }
        return image;   // we rewrite the original array as it's no longer needed
    }
}
