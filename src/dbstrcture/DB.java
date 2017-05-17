package dbstrcture;



/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;


/**
 *
 * @author ZhangJiangwei
 */
public class DB {
    public Table[] tables;
    public HashMap<String, Integer> tableMapping = new HashMap<>();
   
    public HashMap<String, Integer> tableSize = new HashMap<>();
    public HashMap<String, Integer> fkSize = new HashMap<>();
    public HashMap<String, String> tableType = new HashMap<>();
    public HashMap<String, ArrayList<String>> table_attr_map = new HashMap<>();
    public HashMap<String, ArrayList<ComKey>> fkRelation  = new HashMap<>();
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle = new HashMap<>();
    public HashMap<ComKey, Integer> chainKey_map = new HashMap<>();
    
    public int getTableNum(String table){
        return tableMapping.get(table);
    }
    
    
    
    public void dropFKs() {
        for (Table t : tables) t.fks = null;
    }
    
    
    public void loadMap( String input) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(input));
        while (scanner.hasNext()) {
            String[] splits = scanner.nextLine().trim().split("\\s+");
            int num = Integer.parseInt(splits[1]);
            String tableName = splits[0];
            String type = splits[3];
            this.fkSize.put(tableName, num);
            this.tableType.put(tableName, type);
            this.tableSize.put(tableName, Integer.parseInt(splits[2]));
            String[] attributes = scanner.nextLine().split("\\s+");
            ArrayList<String> arr = new ArrayList<>();
            for (int i = 0; i < num + 1 && i < attributes.length; i++) {
                arr.add(attributes[i]);
            }
            table_attr_map.put(tableName, arr);
        }
        scanner.close();
    }
    
      
    //Return the map which indicates the FK referencing relation. Key is the table name, Value is the referenced table and the corrspoondoing index.
    public void load_fkRelation() {
       // HashMap<String, ArrayList<ComKey>> result = new HashMap<>();
        for (Map.Entry<String, ArrayList<String>> table : table_attr_map.entrySet()) {
            String tName = table.getKey();
            ArrayList<String> fks = new ArrayList<>();

            for (int i = 1; i < table.getValue().size(); i++) {
                String aName = table.getValue().get(i);
                if (aName.contains("-")) {
                    String[] temp = aName.split("-");
                    table_attr_map.get(tName).set(i, temp[0]);
                    String[] temps = temp[1].split(":");
                    ComKey comkey = new ComKey();
                    comkey.sourceTable = temps[0];
                    comkey.referencingTable = tName;
                    comkey.referenceposition = i;
                    int chainKey_size =chainKey_map.size();
                    chainKey_map.put(comkey, chainKey_size);
                   
                    if (!(tName.equals("socialgraph") && i == 2)) {
                        fks.add(temps[0]);
                        if (!this.fkRelation.containsKey(tName)) {
                            fkRelation.put(tName, new ArrayList<ComKey>());
                        }
                        if (!fkRelation.get(tName).contains(comkey)) {
                            fkRelation.get(tName).add(comkey);
                        }
                    } else {
                        fkRelation.get(tName).add(fkRelation.get(tName).get(0));
                    }
                }
            }

        }

        
    }
    public void initial_loading(String filePath, int leading, boolean ignoreFirst,String delim ) throws FileNotFoundException, IOException{
        loadMap(filePath);
        System.out.println("====================Map Loaded=============================");
        load_fkRelation();
      
        System.out.println("=========================loadTuples=======================");
       
        processMergeDegreeTitle();
        loadTuple(filePath, leading, ignoreFirst, delim);
    }
    public void loadTuple( String filePath, int leading, boolean ignoreFirst,String delim ) throws FileNotFoundException, IOException {

        ArrayList<Thread> liss = new ArrayList<>();
        this.tables
                = new Table[this.table_attr_map.size()];
        int count = 0;
        for (String table : this.table_attr_map.keySet()) {
            ParaReader pr = new ParaReader(table);
            pr.tables = this.tables;
            this.tableMapping.put(table, count);
            pr.tableNum = count;
            pr.leading = leading;
            count++;
             pr.ignoreFirst = ignoreFirst;
            pr.delim = delim;
            pr.filePath = filePath;
            pr.fkSize = this.fkSize.get(table);
            pr.tableSize = this.tableSize;
            Thread thr = new Thread(pr);

            liss.add(thr);
            thr.start();
        }
        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
         //       Logger.getLogger(MutualComment.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void processMergeDegreeTitle() {
         HashSet<ComKey> temp = new HashSet<>();
        for (ArrayList<ComKey> cks : fkRelation.values()) {
            temp.addAll(cks);
        }
        for (ComKey key : temp) {
            if (!mergedDegreeTitle.containsKey(key.sourceTable)) {
                mergedDegreeTitle.put(key.sourceTable, new ArrayList<ComKey>());
            }
            if (!mergedDegreeTitle.get(key.sourceTable).contains(key)) {
                mergedDegreeTitle.get(key.sourceTable).add(key);
            }
        }
    }

}
