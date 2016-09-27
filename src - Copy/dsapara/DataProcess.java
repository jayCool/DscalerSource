/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dsapara;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 *
 * @author workshop
 */
public class DataProcess {
        DataProcess(){}
       void addDelimiter(String file) throws FileNotFoundException, IOException{
           FileInputStream  input = new FileInputStream (file);
            BufferedReader bf =new BufferedReader(new InputStreamReader(input),100000);
            String s= bf.readLine();
          //  PrintWriter pw2 = new PrintWriter(new File(file+"clean"));
            PrintWriter pw = new PrintWriter(new File(file+"comma"));
            int size = s.split("\\s+").length;
            while (s!=null){
                    String temp[] =s.split("\\s+");
        //    System.out.println(temp.length);
                    for (int i=0;i<size-1;i++){
                    pw.print(temp[i]+";");
                    }
                    for (int i=size-1;i<temp.length;i++){
                      pw.print(temp[i]+" ");
                  
                    }
          pw.println();
           s= bf.readLine();
            }
            pw.close();
           bf.close();
              
     }
   public void     printNullValue(String file) throws FileNotFoundException, IOException{
            FileInputStream  input = new FileInputStream (file);
            BufferedReader bf =new BufferedReader(new InputStreamReader(input),100000);
            String s= bf.readLine();
            PrintWriter pw2 = new PrintWriter(new File(file+"clean"));
            PrintWriter pw = new PrintWriter(new File(file+"null"));
            int size = s.split("\\s+").length;
            System.out.println(size);
            while (s!=null){
            String temp[] =s.split("\\s+");
        //    System.out.println(temp.length);
            if (temp.length<size)
            pw.println(s);
            else pw2.println(s.trim());
            s=bf.readLine();
            }
            pw.close();
            bf.close();
            pw2.close();
                    
        }
   
   public void     combineValue(String file1,String file2) throws FileNotFoundException, IOException{
       PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file1, true)) );
       Scanner scanner = new Scanner(new File(file2));
       while (scanner.hasNext()){
           String s = scanner.nextLine().trim();
           pw.println(s+"   null    null");
       }
       pw.close();
       scanner.close();
   }
   
   public void addID(String file) throws FileNotFoundException, IOException{
        FileInputStream  input = new FileInputStream (file);
            BufferedReader bf =new BufferedReader(new InputStreamReader(input),100000);
            String s= bf.readLine();
            PrintWriter pw2 = new PrintWriter(new File(file+"processed"));
            int size = s.split("\\s+").length;
            System.out.println(size);
            int id=0;
            while (s!=null){
            String temp[] =s.split("\\s+");
        //    System.out.println(temp.length);
            if (temp.length<size)
            {}
            else pw2.println(id+"    "+s.trim());
            s=bf.readLine();
            id++;
            }
            pw2.close();
            bf.close();
                    
   }
}
