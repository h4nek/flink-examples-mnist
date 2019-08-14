import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

public class FilterOnes {
    public static void main(String[] args) throws Exception {
        /*Set up the environment*/
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String imagesPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-images.idx3-ubyte";
        String labelsPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-labels.idx1-ubyte";
        MNISTFileInputFormat imagesHandler = new MNISTFileInputFormat(imagesPath);
        /*Initialize the DataSets from source files*/
        DataSet<byte[]> images = env.readFile(imagesHandler, imagesPath);
        DataSet<byte[]> labels = env.readFile(new MNISTFileInputFormat(labelsPath), labelsPath);
        /*Perform transformations*/
        DataSet<byte[]> ones = images.join(labels).where(new SelectIndex()).equalTo(new SelectIndex()).filter(x -> x.f1[0] == 1).map(x -> x.f0);
//        System.out.println(ones.collect().size());  //TEST -- 6742
        /*Write the DataSets into output files through sinks*/
        String outputPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\output\\outputOnes\\";
//        ones.write(new PngFileOutputFormat(imagesHandler.getNumCols(), imagesHandler.getNumRows(), "one"), outputPath); // problem -- only 591 files are written
        ones.output(new PngOutputFormat<>(outputPath, "one", imagesHandler.getNumCols(), imagesHandler.getNumRows()));
        /*Execute the Flink job*/
        env.execute("Filter Images of Ones");
    }

    static class SelectIndex implements KeySelector<byte[], Integer> {
        private int idx;
        
        @Override
        public Integer getKey(byte[] value) throws Exception {
            ++idx;
            return idx;
        }
    }
}
