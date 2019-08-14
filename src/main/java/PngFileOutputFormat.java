import com.sun.imageio.plugins.png.PNGImageWriter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class PngFileOutputFormat extends FileOutputFormat<byte[]> {

    private BufferedImage image;
    private WritableRaster raster;
    
    private final int numCols;
    private final int numRows;
    private int numRecord;
    private final String fileName;
    private String directoryPath;
    
    PngFileOutputFormat(int numCols, int numRows, String fileName) {
//        super(new Path(fileName));    // we implement our own method of writing which this'd interfere with
        
        this.numCols = numCols;
        this.numRows = numRows;
        this.fileName = fileName;
        
        setOutputDirectoryMode(OutputDirectoryMode.ALWAYS); // output to multiple files
        setWriteMode(FileSystem.WriteMode.OVERWRITE);
        /*RuntimeContext runtimeContext = getRuntimeContext();
        runtimeContext.getNumberOfParallelSubtasks();
        setRuntimeContext();*/
    }

    @Override
    public void initializeGlobal(int parallelism) throws IOException {
        super.initializeGlobal(1);  // we don't want parallel writing (for now)
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
//        super.open(taskNumber, numTasks);

        image = new BufferedImage(numCols, numRows, BufferedImage.TYPE_BYTE_GRAY);
        raster = image.getRaster();

        System.out.println("output file path: " + getOutputFilePath());
        System.out.println("write mode: " + getWriteMode());
        System.out.println("output directory mode: " + getOutputDirectoryMode());

        System.out.println("task number:" + taskNumber);
        System.out.println("num tasks: " + numTasks);   //TEST -- 12
        System.out.println("directory file name: " + getDirectoryFileName(taskNumber));
        
        directoryPath = getOutputFilePath().toString();
    }

    @Override
    public void writeRecord(byte[] record) throws IOException {
        raster.setDataElements(0, 0, numCols, numRows, record);
        image.setData(raster);

        ++numRecord;
        setOutputFilePath(new Path(directoryPath + '/' + numRecord + '_' + fileName + ".png"));
        System.out.println("Writing image nr. " + numRecord);
        System.out.println("current output path: " +getOutputFilePath());
        Path outputFilePath = new Path(directoryPath + '/' + numRecord + '_' + fileName + ".png");
//        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(stream);
//        ImageOutputStream imageOutputStream = ImageIO.createImageOutputStream(bufferedOutputStream);
//        ImageWriter writer = new PNGImageWriter(imageOutputStream);
        File outputFile = new File(outputFilePath.toString());
        outputFile.getParentFile().mkdirs();  // make any parent directories that don't exist
        ImageIO.write(image, "png", outputFile);
//        stream.write(record);
//        stream.flush();
//        System.out.println("The file should be written to: " + outputFilePath);
//        System.out.println("Suggested path is: " + outputFilePath.toString() + '/' + numRecord + '_' + fileName + ".png");//TEST
//        File outputFile = new File(outputFilePath.toString() + '/' + numRecord + '_' + fileName + ".png");
//        try {
//            System.out.println("Writing image nr. " + i + "...");
//            ImageIO.write(image, "png", outputFile);
//        }
//        catch (IOException e) {
//            System.err.println("Could not write the image nr. " + i);
//        }
    }
}
