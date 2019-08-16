import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

public class FilterOnes {
    public static void main(String[] args) throws Exception {
        /*Set up the environment*/
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String imagesPath = MirrorImages.class.getResource("input/train-images.idx3-ubyte").getPath();
        String labelsPath = MirrorImages.class.getResource("input/train-labels.idx1-ubyte").getPath();
        MNISTFileInputFormat imagesHandler = new MNISTFileInputFormat(imagesPath);
        
        /*Initialize the DataSets from source files*/
        DataSet<byte[]> images = env.readFile(imagesHandler, imagesPath);
        DataSet<byte[]> labels = env.readFile(new MNISTFileInputFormat(labelsPath), labelsPath);
        
        /*Perform transformations*/
        DataSet<byte[]> ones = images.join(labels).where(new SelectIndex()).equalTo(new SelectIndex()).filter(x -> x.f1[0] == 1).map(x -> x.f0);
        
        /*Write the DataSets into output files through sinks*/
        String outputPath = System.getProperty("user.dir") + "\\output\\outputOnes\\";
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
