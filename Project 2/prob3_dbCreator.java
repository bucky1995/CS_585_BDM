import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class prob2_dbCreator {
    static int point_Num = 12000000; // Num of Points
    static Random rnd = new Random();

    public static void main(String[] args) throws IOException {
        System.out.println("Data set creator!");
        CreatDataset_P("P.csv");
    }

    public static void CreatDataset_P(String filePath) throws IOException {
        File file = new File(filePath);
        // creates the file
        file.createNewFile();
        // creates a FileWriter Object
        FileWriter writer = new FileWriter(file);

        for (int i = 1; i <= point_Num; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(rnd.nextInt(10000) + 1);// X
            sb.append(",");
            sb.append(rnd.nextInt(10000) + 1);// Y

            sb.append("\n");
            writer.write(sb.toString());
            writer.flush();

        }

        writer.close();
    }
}
