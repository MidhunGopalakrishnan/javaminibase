package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class ScannerTest {
    public static void main(String[] args) {
        File obj = new File("src/data/phase3data11.txt");
        try {
            Scanner s = new Scanner(obj);
            while(s.hasNextLine()) {
                String dataLine = s.nextLine();
                String[] dataArray = dataLine.split(" ");
                for (int i =0; i< dataArray.length;i++){
                    System.out.println(dataArray[i]);
                }
                System.out.println("Line completed");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
