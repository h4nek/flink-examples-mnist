import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.omg.CORBA.Environment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A class for reading the MNIST Dataset.
 * It distinguishes between the image and label file.
 *
 */
public class MNISTFileInputFormat extends FileInputFormat<byte[]> {
    
    private final int numImages;    // signifies number of images for the image file and number of labels for the label file
    private final int numRows;
    private final int numCols;
    private final int numDims;  // stores the number of dimensions of the dataset (3 for images, 1 for labels)
    private int numRead = 0;

    public int getNumDims() {
        return numDims;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNumCols() {
        return numCols;
    }
    
    MNISTFileInputFormat(String filePath) throws Exception {
        super();
        unsplittable = true;    // ensure that the number of input splits (parallelism) == 1
//        System.out.println("In the input constructor.");
        InputStream is = new FileInputStream(filePath); // the FileInputFormat stream is not yet initialized, so we'll
                                                        // use another/custom one

//        is = new FileInputStream(fileSplit.getPath().toString().substring(6));
        is.skip(3); // we don't need the first 3 bytes of the "magic number" (int)
        
        numDims = is.read();  // use the 4th byte to determine the number of dimensions

        numImages = readNextInt(is);
        
        if (numDims == 3) {
            numRows = readNextInt(is);
            numCols = readNextInt(is);
        }
        else {  // we'll read the items into 1x1 byte arrays
            numRows = 1;
            numCols = 1;
        }
        
        is.close();
    }

//    @Override
//    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
//        FileInputSplit[] splits = super.createInputSplits(minNumSplits);
//
//
//        if (stream == null) {
//            System.out.println("STREAM IS NULL");
//            return splits;
//        }
//        
//        stream.skip(4); // we don't need the "magic number" (int)
//
//        numImages = readNextInt(stream);
//        numRows = readNextInt(stream);
//        numCols = readNextInt(stream);
//        
//        return splits;
//    }


//    @Override
//    public void configure(Configuration parameters) {
//        super.configure(parameters);
//        
//        if (stream == null) {
//            System.out.println("STREAM IS NULL");
//            return;
//        }
//        try {
//            stream.skip(4); // we don't need the "magic number" (int)
//
////        numImages = readNextInt(is);
//            numImages = readNextInt(stream);
////        numRows = readNextInt(is);
//            numRows = readNextInt(stream);
////        numCols = readNextInt(is);
//            numCols = readNextInt(stream);
//        }
//        catch (IOException e) {
//            System.err.println("Error when reading the file header!");
//            System.exit(-5);
//        }
//    }

    /**
     * Used for reading integers with specific information at the beginning of the MNIST Dataset.
     * //TODO Try to read the data in parallel
     * @return the following 4 bytes read as an int.
     */
    private int readNextInt(InputStream is) throws IOException {
        return is.read() << 24 | is.read() << 16 | is.read() << 8 | is.read();
    }
    
    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        super.open(fileSplit);
//        System.out.println("We're opening!");//TEST
//        System.out.println(extractFileExtension(fileSplit.toString())); //TEST
//        File file = new File(fileSplit.getPath().toString().substring(6));
//        is = new FileInputStream(file);
        
//        is = new FileInputStream(fileSplit.getPath().toString().substring(6));
        
//        is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileSplit.getPath().toString());
//        if (is == null) {   // the file could not be found
//            System.err.println("The file in " + fileSplit.getPath().toString() + " has not been found!");
//            close();
//            return;
//        }
//        byte[] integerNum = new byte[4];
//        ByteBuffer buffer = ByteBuffer.wrap(integerNum);
//        is.skip(4); // we don't need the "magic number"

//            System.out.println(is.read() << 24 | is.read() << 16 | is.read() << 8 | is.read()); // another way to read (big endian) ints
        
//        readNextInt(is); // we don't need the "magic number"
//        stream.skip(4); // we don't need the "magic number" (int)
//
////        numImages = readNextInt(is);
//        numImages = readNextInt(stream);
////        numRows = readNextInt(is);
//        numRows = readNextInt(stream);
////        numCols = readNextInt(is);
//        numCols = readNextInt(stream);

        stream.skip(4*(numDims+1));   // skip an int for each dimension + the magic number  
        System.out.println("num of images: " + numImages);    //TEST -- 60 000
        System.out.println("num of dims: " + numDims);      //TEST
        System.out.println("num of rows: " + numRows);    //TEST -- 28
        System.out.println("num of cols: " + numCols);    //TEST -- 28
        
//        Configuration configuration = Configuration.getConfiguration();
//        configuration.getParameters();
    }

    @Override
    public byte[] nextRecord(byte[] reuse) throws IOException {
//        byte[] matrix = new byte[numRows*numCols];
        if (reuse == null || reuse.length == 0) {
            reuse = new byte[numRows*numCols];
        }
//        is.read(matrix); // read all the bytes filling the matrix
        ++numRead;
//        System.out.println("Reading image nr. " + numRead);
        stream.read(reuse); // read all the bytes filling the matrix
//        System.out.println(reuse.length);
//        for (byte i : reuse) {
//            System.out.println(i);
//        }
//        return matrix;
        return reuse;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return numRead >= numImages;
    }
}
