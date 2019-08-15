import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

public class FilterZeroes {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String imagesPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-images.idx3-ubyte";
        String labelsPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-labels.idx1-ubyte";
        MNISTFileInputFormat imagesHandler = new MNISTFileInputFormat(imagesPath);
        DataSet<byte[]> images = env.readFile(imagesHandler, imagesPath).setParallelism(1);
        DataSet<byte[]> labels = env.readFile(new MNISTFileInputFormat(labelsPath), labelsPath).setParallelism(1);
        
        DataSet<byte[]> zeroes = images.join(labels)
                .where(new SelectIndex())
                .equalTo(new SelectIndex())
                .filter(x -> x.f1[0] == 0)    // filter only images representing a '0'
                .map(x -> x.f0);  // get rid of the labels

        String outputPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\output\\outputZeroes\\";
        zeroes.output(new PngOutputFormat<>(outputPath, "zero", imagesHandler.getNumCols(), imagesHandler.getNumRows()));
        env.execute("Filter Images of Zeroes");
    }

    /**
     * Simply assigns an index to each incoming element, starting from 1.
     */
    public static class SelectIndex implements KeySelector<byte[], Integer> {  
        private int idx;
        
        @Override
        public Integer getKey(byte[] value) throws Exception {
            idx++;
            return idx;
        }
    }
}
