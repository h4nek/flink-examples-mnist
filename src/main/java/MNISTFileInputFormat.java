import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;

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

    public int getNumRows() {
        return numRows;
    }

    public int getNumCols() {
        return numCols;
    }
    
    MNISTFileInputFormat(String filePath) throws Exception {
        super();
        unsplittable = true;    // ensure that the number of input splits (parallelism) == 1
        InputStream is = new FileInputStream(filePath); // the FileInputFormat stream is not yet initialized, so we'll
                                                        // use another/custom one

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

        stream.skip(4*(numDims+1));   // skip an int for each dimension + the magic number
    }

    @Override
    public byte[] nextRecord(byte[] reuse) throws IOException {
        if (reuse == null || reuse.length == 0) {
            reuse = new byte[numRows*numCols];
        }
        
        ++numRead;
        stream.read(reuse); // read all the bytes filling the matrix
        
        return reuse;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return numRead >= numImages;
    }
}
