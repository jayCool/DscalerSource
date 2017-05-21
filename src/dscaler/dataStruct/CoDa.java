/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dscaler.dataStruct;

import dbstrcture.ComKey;
import dbstrcture.DB;
import dsapara.Dscaler;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ZhangJiangwei
 */
public class CoDa {
    public HashMap<ComKey, int[]> idCounts = new HashMap<>();
    public HashMap<ComKey, HashMap<Integer, Integer>> idFreqDis = new HashMap<>();
 
    public HashMap<String, ArrayList<ComKey>> jointDegreeTable = new HashMap<>(); //key is the table A, values are the tables REFERENCING table A
    
    public HashMap<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> jointDegrees = new HashMap<>();    
    public HashMap<ArrayList<ComKey>,HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseJointDegrees = new HashMap<>();    
    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeDis  = new HashMap<>();
 
    public HashMap<String, ArrayList<ArrayList<ArrayList<Integer>>>> idRVs = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseRVs = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> rvDis = new HashMap<>();
    
     
    public HashMap<String, ArrayList<ArrayList<ArrayList<Integer>>>> idKVs = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseKV = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> kvDis = new HashMap<>();
    
    public void dropKVDis(){
        this.kvDis = null;
    }
   
    //key [table1,table2], table 1 is the source table, table 2 is the referceing table.
    public void loadKeyCounts(HashMap<String, ArrayList<ComKey>> fkRelation, DB originalDB) {
        for (Map.Entry<String, ArrayList<ComKey>> entry : fkRelation.entrySet()) {
            int count = 0;
            for (ComKey ck : entry.getValue()) {

                String tableName = entry.getKey();
                int colNum = ck.referenceposition;
                if (tableName.equals("socialgraph") && count == 1) {
                    colNum = ck.referenceposition + 1;
                    continue;
                }
                int tableNum = originalDB.tableMapping.get(tableName);

                String srcTable = ck.sourceTable;
                int srcNum = originalDB.getTableNum(srcTable);

                int[] idcounts = new int[originalDB.tables[srcNum].fks.length];

                for (int[] arr : originalDB.tables[tableNum].fks) {
                    int id = arr[colNum - 1];
                    idcounts[id] += 1;
                }
                idCounts.put(ck, idcounts);
                count++;
            }
        }
    }
    
    public void processJointDegreeTable(Set<ComKey> comKeys) {
        for (ComKey ck : comKeys) {
            if (!jointDegreeTable.containsKey(ck.sourceTable)) {
                jointDegreeTable.put(ck.sourceTable, new ArrayList<ComKey>());
            }
            if (!jointDegreeTable.get(ck.sourceTable).contains(ck)) {
                jointDegreeTable.get(ck.sourceTable).add(ck);
            }
        }
    }
    
    public void processJointDegree(DB originalDB
    ) {
        for (Map.Entry<String, ArrayList<ComKey>> entry : jointDegreeTable.entrySet()) {
            ArrayList<ComKey> keys = new ArrayList<>();
            keys.addAll(entry.getValue());
            HashMap<ArrayList<Integer>, ArrayList<Integer>> jointDegreeIdMap = new HashMap<>();
            String tableName = entry.getKey();
            int tableNum = originalDB.getTableNum(tableName);
            ArrayList<ArrayList<Integer>> allJointDegrees = new ArrayList<>(originalDB.tables[tableNum].fks.length);

            for (int pkid = 0; pkid < originalDB.tables[tableNum].fks.length; pkid++) {
                ArrayList<Integer> degrees = new ArrayList<>(keys.size());

                for (int o = 0; o < keys.size(); o++) {
                    ComKey ck = keys.get(o);
                    degrees.add(idCounts.get(ck)[pkid]);
                }
                if (!jointDegreeIdMap.containsKey(degrees)) {
                    jointDegreeIdMap.put(degrees, new ArrayList<Integer>());
                }
                jointDegreeIdMap.get(degrees).add(pkid);
                allJointDegrees.add(degrees);
            }

            reverseJointDegrees.put(keys, jointDegreeIdMap);
            jointDegrees.put(keys, allJointDegrees);
        }
    }
    
    
    public void processRV(
            DB originalDB
    ) throws FileNotFoundException {
        ArrayList<Thread> liss = new ArrayList<>();
        for (Map.Entry<String, ArrayList<ComKey>> entry : originalDB.fkRelation.entrySet()) {
            ParaLoadCorr plc = new ParaLoadCorr();
            plc.originalDB = originalDB;
            plc.entry = entry;
            plc.jointDegreeTable = jointDegreeTable;
            plc.originalCoDa = this;

            Thread thr = new Thread(plc);
            liss.add(thr);
            thr.start();

        }

        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    
     public void processKV() {
        for (Map.Entry<String, ArrayList<ArrayList<ArrayList<Integer>>>> rv : idRVs.entrySet()) {
            String table = rv.getKey();
            for (Map.Entry<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> jointDegree : jointDegrees.entrySet()) {
                if (table.equals(jointDegree.getKey().get(0).sourceTable)) {
                    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvMap = new HashMap<>();
                    HashMap<ArrayList<ArrayList<Integer>>, Integer> kvDisMap = new HashMap<>();
               
                    for (int i = 0; i < jointDegree.getValue().size(); i++) {
                        ArrayList<ArrayList<Integer>> kv = new ArrayList<>();
                        kv.add(jointDegree.getValue().get(i));
                        kv.addAll(rv.getValue().get(i));

                        if (!kvMap.containsKey(kv)) {
                            kvMap.put(kv, new ArrayList<Integer>());
                        }
                        kvMap.get(kv).add(i);

                        if (!kvDisMap.containsKey(kv)) {
                            kvDisMap.put(kv, 1);
                        } else {
                            kvDisMap.put(kv, 1 + kvDisMap.get(kv));
                        }
                    }
                  kvDis.put(table, kvDisMap);
                  reverseKV.put(table, kvMap);
                }
            }
        }
        idRVs = null;

    }
    
      public void processIdFrequency() {
        for (Map.Entry<ComKey, int[]> entry : idCounts.entrySet()) {
            HashMap<Integer, Integer> freqMap = new HashMap<>();
            for (int c : entry.getValue()) {
                if (!freqMap.containsKey(c)) {
                    freqMap.put(c, 1);
                } else {
                    freqMap.put(c, 1 + freqMap.get(c));
                }
            }
            System.err.println("frequencies:" + entry.getKey().toString() + "\t" + freqMap);
            idFreqDis.put(entry.getKey(), freqMap);
        }
    }
      
    public void processJointDis() {
        for (Map.Entry<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> entry : jointDegrees.entrySet()) {
            HashMap<ArrayList<Integer>, Integer> degreeFreq = new HashMap<>();
            for (int i = 0; i < entry.getValue().size(); i++) {
                if (!degreeFreq.containsKey(entry.getValue().get(i))) {
                    degreeFreq.put(entry.getValue().get(i), 1);
                } else {
                    degreeFreq.put(entry.getValue().get(i), 1 + degreeFreq.get(entry.getValue().get(i)));
                }
            }
            jointDegreeDis.put(entry.getKey(), degreeFreq);
        }
        jointDegrees = null;
        idCounts = null;

    }  

    public void dropIdFreqDis() {
    idFreqDis = null; 
    }
}
