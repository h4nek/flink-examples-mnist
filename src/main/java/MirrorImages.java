import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.net.URL;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MirrorImages {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        URL url = MirrorImages.class.getResource("input/train-images.idx3-ubyte");
        String filePath = url.getPath();    // "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-images.idx3-ubyte"
        
        MNISTFileInputFormat mnistInputHandler = new MNISTFileInputFormat(filePath);
        DataSet<byte[]> matrices = env.readFile(mnistInputHandler, filePath);
        
        /*Printing some info about the source.*/
//        System.out.println("num of input files: " + mnistInputHandler.getFilePaths().length);// TEST -- 1 | the file is apparently set by the readFile() function
//        for (Path path : mnistInputHandler.getFilePaths()) {    // TESTING -- /D:/Programy/BachelorThesis/Tests/JavaApacheFlink/MNIST_Database/src/main/resources/input/train-images.idx3-ubyte
//            System.out.println("the path: " + path.getPath());
//        }
//        System.out.println("min. split size: " + mnistInputHandler.getMinSplitSize()); //TEST
//        System.out.println("num. of splits: " + mnistInputHandler.getNumSplits());  //TEST
//        System.out.println("opening timeout: " + mnistInputHandler.getOpenTimeout());   //TEST -- 300 000
//        System.out.println("is recursive directory traversal enabled? : " + mnistInputHandler.getNestedFileEnumeration());  //TEST -- false
//
//        System.out.println("start of the current split: " + mnistInputHandler.getSplitStart()); //TEST
//        System.out.println("current split's (remaining) length: " + mnistInputHandler.getSplitLength());    //TEST
//        System.out.println("statistics? : " + mnistInputHandler.getStatistics(null));   //TEST
//        System.out.println("num of years ~ ? (modAt) : " + Time.milliseconds(mnistInputHandler.getStatistics(null).getLastModificationTime()).toMilliseconds()/(1000.0 * 60 * 60 * 24 * 365));
//        System.out.println("Date of File: " + new Date(mnistInputHandler.getStatistics(null).getLastModificationTime())); // TEST -- Mon Nov 18 16:36:26 CET 1996
//
//        System.out.println("read all elements? : " + mnistInputHandler.reachedEnd());   //TEST

        int numRows = mnistInputHandler.getNumRows();
        int numCols = mnistInputHandler.getNumCols();
        
        DataSet<byte[]> mirrors = matrices.map(new MirrorImageMap(numRows, numCols));
        
        String outputPath = System.getProperty("user.dir") + "\\output\\outputMirrors\\";
        mirrors.output(new PngOutputFormat<>(outputPath,"mirror", numCols, numRows));

        env.execute();
        
        /* A working implementation using non-Flink I/O. */
//        AtomicInteger height = new AtomicInteger();
//        AtomicInteger width = new AtomicInteger();
//        MNISTDataSetIO mnistDataSetIO = new MNISTDataSetIO();
//        DataSet<byte[]> matrices = env.fromCollection(
//                mnistDataSetIO.readIDX("input/train-images.idx3-ubyte", width, height));
//
//        List<byte[]> mirrors = matrices.map(new MirrorImageMap(height.get(), width.get())).collect();
//        System.out.println(mirrors.size()); // 60 000
//        System.out.println(mirrors);    // prints out the references to individual matrices - should start in no time
//
//        mnistDataSetIO.saveImages("output/outputMirrors", "mirror", mirrors, 
//                height.get(), width.get());
    }
}

/**
 * This method flips the image horizontally
 */
class MirrorImageMap implements MapFunction<byte[], byte[]> {
    
    private int height;
    private int width;
    
    MirrorImageMap(int height, int width) { // we need to pass and store additional (invariant) information
        this.height = height;
        this.width = width;
    }
    
    @Override
    public byte[] map(byte[] image) throws Exception {
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width/2; j++) { // we'll only iterate over the left part of the image, swapping it with the right part
                // generally, if the width is even, then we iterate over exactly half of the matrix elements
                // if it's an odd number, we'll exclude the elements in the middle column, which would get swapped with
                // themselves, so we don't actually need to access them
                byte pixel = image[i*width + j];    // get the currently processed element
                int mirrorPos = i*width + width - 1 - j;    // calculate the new position after the mirroring
                image[i*width + j] = image[mirrorPos];    // replace it by an element on the same row, but "opposite" column
                image[mirrorPos] = pixel; // finally, replace the other element with the stored one
            }
        }
        return image;
    }
}
