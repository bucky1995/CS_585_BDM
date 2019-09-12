import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class dbCreator {
    static int cusNum = 50000; // Num of customers
    static int transNum = 5000000; // Num of transactions
    static String chars = "abcdefghijklmnopqrstuvwxyz";
    static Random rnd = new Random();
    // Create the desired customer and transactions into two files
    public static void main(String[] args) throws IOException {
        System.out.println("Data set creator!");
        writeCustomerData("customer.csv");
	writeTransactionData("transaction.csv");

    }


    public static void writeCustomerData(String filePath) throws IOException {
        File file = new File(filePath);
        // creates the file
        file.createNewFile();
        // creates a FileWriter Object
        FileWriter writer = new FileWriter(file);


        for(int i =1; i <= cusNum; i++){
            StringBuilder sb = new StringBuilder();
            sb.append(i);
            sb.append(",");

            // Random sequence of characters
            int nameLength = rnd.nextInt(11)+10;
            for(int j = 0; j < nameLength; j++){
                sb.append(chars.charAt(rnd.nextInt(chars.length())));
            }
            sb.append(",");

            //Random age from 10 to 70
            sb.append(rnd.nextInt(61)+10);
            sb.append(",");

            //Gender either male or female
            if(rnd.nextInt(2) == 1){
                sb.append("male");
            }else{
                sb.append("female");
            }
            sb.append(",");

            //Random country code from 1 to 10
            sb.append(rnd.nextInt(10)+1);
            sb.append(",");

            //Random Salary float from 100 to 10000
            sb.append(rnd.nextFloat()*9901+100);
            sb.append("\n");
            writer.write(sb.toString());
            writer.flush();
        }
        writer.close();
    }

    public static void writeTransactionData(String filePath) throws IOException {
        File file = new File(filePath);
        // creates the file
        file.createNewFile();
        // creates a FileWriter Object
        FileWriter writer = new FileWriter(file);
        int customerId = 1;
        for(int i =1; i <= transNum; i++){
            StringBuilder sb = new StringBuilder();
            sb.append(i);
            sb.append(",");

            //Custom Id of average of 100
            customerId = rnd.nextInt(50000)+1;
            sb.append(customerId);
            sb.append(",");


            //Random Transtotal from 10 to 1000 float
            sb.append(rnd.nextFloat()*990+10);
            sb.append(",");

            //Random number item from 1 to 10
            sb.append(rnd.nextInt(10)+1);
            sb.append(",");

            // Random sequence of characters from 20 to 50
            int tranDescLength = rnd.nextInt(31)+20;
            for(int j = 0; j < tranDescLength; j++){
                sb.append(chars.charAt(rnd.nextInt(chars.length())));
            }

            sb.append("\n");
            writer.write(sb.toString());
            writer.flush();
        }
        writer.close();
    }

}
