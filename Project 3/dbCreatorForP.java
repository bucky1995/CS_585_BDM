import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class dbCreatorForP {
    static int size = 1000; // Num of customers
    static Random rnd = new Random();
    // Create the desired customer and transactions into two files
    public static void main(String[] args) throws IOException {
        System.out.println("Data set creator!");
        writeMatrixData("N.csv");
        writeMatrixData("M.csv");
    }


    public static void writeMatrixData(String filePath) throws IOException {
        File file = new File(filePath);
        // creates the file
        file.createNewFile();
        // creates a FileWriter Object
        FileWriter writer = new FileWriter(file);


        for(int i =0; i < size; i++){
            for(int j = 0; j < size;j++){
                StringBuilder sb = new StringBuilder();
                sb.append(i);
                sb.append(",");
                sb.append(j);
                sb.append(",");
                sb.append(rnd.nextInt(10)+1);
                sb.append("\n");
                writer.write(sb.toString());
                writer.flush();
            }
        }
        writer.close();
    }


}
