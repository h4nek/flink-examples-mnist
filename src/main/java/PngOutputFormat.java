import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A more generic output format for outputing each element into a separate PNG file.
 * The type of elements is generic even though we'll only use it on byte[] images.
 * @param <T>
 */
public class PngOutputFormat<T> extends RichOutputFormat<T> {
    
    private final String directoryPath;
    private final String suffix;
    private static volatile AtomicInteger numFile;
    private final int numCols;
    private final int numRows;

    private BufferedImage image;
    private WritableRaster raster;

    public PngOutputFormat(String directoryPath, String suffix, int numCols, int numRows) {
        this.directoryPath = directoryPath;
        this.suffix = suffix;
        this.numCols = numCols;
        this.numRows = numRows;
        numFile = new AtomicInteger();
    }

    @Override
    public void configure(Configuration parameters) {
        // for now, we pass the parameters through the constructor
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        image = new BufferedImage(numCols, numRows, BufferedImage.TYPE_BYTE_GRAY);
        raster = image.getRaster();
        
        new File(directoryPath).mkdirs();  // make any directories that don't exist
    }

    @Override
    public void writeRecord(T record) throws IOException {
        int numImage = numFile.incrementAndGet();
        String filePath = directoryPath + numImage + '_' + suffix + ".png";

        raster.setDataElements(0, 0, numCols, numRows, record);
        image.setData(raster);
        
        File outputFile = new File(filePath);
        
        ImageIO.write(image, "png", outputFile);
        System.out.println("Image nr. " + numImage + " written!");
    }

    @Override
    public void close() throws IOException {
        // The ImageIO closes the stream after each write, so we don't need to close anything.
    }
}
