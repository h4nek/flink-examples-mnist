import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class MNISTDataSetIO {

    /**
     *  Designed to read the image dataset files stored in IDX format.
     *  
     * @param filename the name of the IDX images file to be read, relative to the resources folder
     * @param width used to store the width (number of columns) of each image; passed as an <code>AtomicInteger</code>
     *             to convey the information
     * @param height used to store the height (number of rows) of each image
     * @return <code>ArrayList</code> of <code>byte[]</code> images (0-255 grayscale)
     */
    public ArrayList<byte[]> readIDX(String filename, AtomicInteger width, AtomicInteger height) {
        ArrayList<byte[]> matrices = new ArrayList<>();
        try {   
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
            byte[] integerNum = new byte[4];
            ByteBuffer buffer = ByteBuffer.wrap(integerNum);
            is.skip(4); // we don't need the "magic number"

//            System.out.println(is.read() << 24 | is.read() << 16 | is.read() << 8 | is.read()); // another way to read (big endian) ints
            is.read(integerNum);
            int numImages = buffer.getInt(0);
            
            is.read(integerNum);
            int numRows = buffer.getInt(0);
            
            is.read(integerNum);
            int numCols = buffer.getInt(0);
            
//            System.out.println(String.valueOf(numImages) + numRows + numCols);
            
            for (int i = 0; i < numImages; ++i) {   // read the images one by one
                byte[] matrix = new byte[numRows*numCols];
                is.read(matrix); // read all the bytes filling the matrix
                matrices.add(matrix);
            }

            width.set(numCols);
            height.set(numRows);
        }
        catch (FileNotFoundException e) {
            System.err.println("No such file named " + filename);
        }
        catch (IOException e) {
            System.err.println("A problem occured when reading the file.");
        }
        
        return matrices;
    }
    
    public void saveImages(String relativePath, String fileName, List<byte[]> matrices, int numRows, int numCols) {
        BufferedImage image = new BufferedImage(numCols, numRows, BufferedImage.TYPE_BYTE_GRAY);
        WritableRaster raster = image.getRaster();
        
        for (int i = 0; i < matrices.size(); ++i) {
            raster.setDataElements(0, 0, numCols, numRows, matrices.get(i));
            image.setData(raster);
            
            File outputFile = new File(relativePath + '/' + i + '_' + fileName + ".png");
            try {
                System.out.println("Writing image nr. " + i + "...");
                ImageIO.write(image, "png", outputFile);
            }
            catch (IOException e) {
                System.err.println("Could not write the image nr. " + i);
            }
        }
    }

    /**
     * Reads, converts and saves the images for both training and testing.
     * @param args
     */
    public static void main(String[] args) {
        MNISTDataSetIO MNISTDataSetIO = new MNISTDataSetIO();
        
        AtomicInteger width = new AtomicInteger();
        AtomicInteger height = new AtomicInteger();
        ArrayList<byte[]> matrices = MNISTDataSetIO.readIDX("input/train-images.idx3-ubyte", width, height);
        System.out.println("The size is " + width + 'X' + height);
        MNISTDataSetIO.saveImages("output/inputImages", "orig", matrices, height.get(), width.get());
    }
}
