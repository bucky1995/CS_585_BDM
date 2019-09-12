import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class prob1_dbCreator {
    static int point_Num = 1200000; // Num of Points
    static int rectangleNum = 5000000; // Num of Rectangle
    static Random rnd = new Random();


    public static void main(String[] args) throws IOException {
        System.out.println("Data set creator!");
        CreatDataset_P("P.csv");
        CreatDataset_R("R.csv");

    }

    public static void CreatDataset_P(String filePath) throws IOException {
        File file = new File(filePath);
        // creates the file
        file.createNewFile();
        // creates a FileWriter Object
        FileWriter writer = new FileWriter(file);

        for (int i = 1; i <= point_Num; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(i); // Point's index;
            sb.append(",");

            sb.append(rnd.nextInt(10000) + 1);// X
            sb.append(",");
            sb.append(rnd.nextInt(10000) + 1);// Y

            sb.append("\n");
            writer.write(sb.toString());
            writer.flush();

        }

        writer.close();
    }

    public static void CreatDataset_R(String filePath) throws IOException {
        File file = new File(filePath);
        // creates the file
        file.createNewFile();
        // creates a FileWriter Object
        FileWriter writer = new FileWriter(file);
        
        for (int i= 1;i<=rectangleNum;i++){
            StringBuilder sb = new StringBuilder();
            sb.append("r");
            sb.append(i);
            sb.append(",");
            int AX = rnd.nextInt(10000) + 1;
            int AY = rnd.nextInt(10000) + 1;
            sb.append(AX );// AX
            sb.append(",");
            sb.append(AY );// AY
            sb.append(",");
            sb.append(rnd.nextInt(20)+1);// h
            sb.append(",");
            sb.append(rnd.nextInt(5)+1);// w
            sb.append("\n");
            writer.write(sb.toString());
            writer.flush();
        }

        writer.close();
    }


}
