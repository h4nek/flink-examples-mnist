import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;

public class FilterAndMirrorZeroesAndOnes {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String imagesPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-images.idx3-ubyte";
        String labelsPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\src\\main\\resources\\input\\train-labels.idx1-ubyte";
        MNISTFileInputFormat imagesHandler = new MNISTFileInputFormat(imagesPath);

        DataSet<byte[]> images = env.readFile(imagesHandler, imagesPath);
        DataSet<byte[]> labels = env.readFile(new MNISTFileInputFormat(labelsPath), labelsPath);

        JoinOperator<byte[], byte[], Tuple2<byte[], byte[]>> joinedSets = images.join(labels)
                                                                                .where(new SelectIndex())
                                                                                .equalTo(new SelectIndex());
        DataSet<byte[]> zeroes = joinedSets.filter(x -> x.f1[0] == 0)
                                           .map(x -> x.f0);
        DataSet<byte[]> ones = joinedSets.filter(x -> x.f1[0] == 1)
                                         .map(x -> x.f0);

        MapFunction<byte[], byte[]> mirrorFunction = new MirrorImageMap(imagesHandler.getNumRows(), imagesHandler.getNumCols());
        DataSet<byte[]> mirroredZeroes = zeroes.map(mirrorFunction);
        DataSet<byte[]> mirroredOnes = ones.map(mirrorFunction);
        
        DataSet<byte[]> mirroredZeroesAndOnes = mirroredZeroes.union(mirroredOnes);
        String outputPath = "D:\\Programy\\BachelorThesis\\Tests\\JavaApacheFlink\\MNIST_Database\\output\\outputMirroredZeroesAndOnes\\";
        mirroredZeroesAndOnes.output(new PngOutputFormat<>(outputPath, "mirroredZeroOrOne",
                                                        imagesHandler.getNumCols(), imagesHandler.getNumRows()));
        
        env.execute("Filter and Mirror Zeroes and Ones");
    }
    
    static class SelectIndex implements KeySelector<byte[], Integer> {
        int idx;
        
        @Override
        public Integer getKey(byte[] value) throws Exception {
            ++idx;
            return idx;
        }
    }
}
