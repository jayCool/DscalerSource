/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.Table;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
//import gnu.trove.hash.map;
import java.util.Map;

/**
 *
 * @author workshop
 */
public class ParaReader implements Runnable {

    String table;
    String delim;
    boolean ignoreFirst;
    String filePath;
    Integer tableSize;
    HashMap<String, Integer> oldTableSize = new HashMap<>();
    Table[] tables;
    HashMap<String, Integer> tupleSize;
    int tableNum;

    ParaReader(String table) {
        this.table = table;
    }

    @Override
    public void run() {
        FileInputStream input1 = null;
        try {
            System.out.println(table + ".txt   ");
            input1 = new FileInputStream(filePath + "/" + table + ".txt");
            BufferedReader scanner = new BufferedReader(new InputStreamReader(input1), 5000000);
            StringTokenizer st;
            Table tb = new Table();
            if (this.ignoreFirst) {
                scanner.readLine().trim().split(delim);
            }
            String input = scanner.readLine().trim();
            int leng = input.trim().split(delim).length;
            int count = 0;

            int[][] fks = new int[this.tupleSize.get(table)][tableSize];
            String[] nonKeys = new String[this.tupleSize.get(table)];
            System.out.println("number of fks: "+this.tableSize);
            while (input != null) { 
                st = new StringTokenizer(input);
                st.nextToken();
                try {
                    for (int i = 1; i < leng && i < this.tableSize + 1; i++) {
                        fks[count][i - 1] = Integer.parseInt(st.nextToken());
                    }

                    if (tableSize + 1 < leng) {
                        String lastLine = "";
                        for (int i = this.tableSize + 1; i < leng; i++) {
                            if (st.hasMoreTokens()) {
                                lastLine += "|" + st.nextToken();
                            } else {
                                lastLine += "|null";
                                System.out.println("input:" + input + "   " + leng);
                              //  System.exit(-1);
                            }
                        }
                        lastLine.trim();
                        nonKeys[count] = lastLine;
                        //System.out.println("lastline:" +lastLine);
                    }
                } catch (NoSuchElementException ne) {
                    
                    System.out.println(input + " Table" + table);
                }

                count++;
                input = scanner.readLine();

            }
            tb.fks = fks;
            tb.nonKeys = nonKeys;
            tb.tableName = table;
            tb.tuplesize =tableSize; 
            tables[this.tableNum] = tb;
            
            oldTableSize.put(table, count);
            System.out.println("Mem=====" + table + "   " + count + "    " + delim + "    " + leng + "  " + "====");
            
            scanner.close();
            scanner = null;
            //   System.gc();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ParaReader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ParaReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                input1.close();
            } catch (IOException ex) {
                Logger.getLogger(ParaReader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

//use split
    public void splitrun() {
        FileInputStream input1 = null;
        try {
            ArrayList<String> arr = new ArrayList<>();
            System.out.println(table + ".txt   ");
            input1 = new FileInputStream(filePath + "/" + table + ".txt");
            BufferedReader scanner = new BufferedReader(new InputStreamReader(input1), 5000000);

            if (this.ignoreFirst) {
                scanner.readLine().trim().split(delim);
            }
            String input = scanner.readLine();
            int leng = input.trim().split(delim).length;
            String[] temp;
            int count = 0;

            HashMap<String, ArrayList<String>> maps = new HashMap<>();

            while (input != null) {
                if (count % 1000000 == 0) {
                    System.gc();
                }
                temp
                        = input.trim().split(delim);
                if (temp.length >= this.tableSize + 1) {
                    arr = new ArrayList<>();
                    for (int i = 1; i < temp.length && i < this.tableSize + 1; i++) {
                        String s = new String(temp[i]);
                        if (!s.trim().isEmpty()) {
                            arr.add(s.trim());
                        } else {
                            arr.add("null");
                        }
                    }
                    if (tableSize + 1 < temp.length) {
                        String lastLine = "";
                        for (int i = this.tableSize + 1; i < temp.length; i++) {
                            lastLine += new String(temp[i]);
                        }
                        lastLine.trim();
                        arr.add(lastLine);
                    }
                    count++;
                    maps.put(temp[0], arr);
                }
                input = scanner.readLine();

            }

            oldTableSize.put(table, count);
            System.out.println("=====" + table + "   " + count + "    " + delim + "    " + leng + "  " + "====");

            scanner.close();
            scanner = null;
            System.gc();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(ParaReader.class
                    .getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ParaReader.class
                    .getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                input1.close();

            } catch (IOException ex) {
                Logger.getLogger(ParaReader.class
                        .getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
