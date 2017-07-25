/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dscaler.dataStruct;

import db.structs.ComKey;
import db.structs.DB;
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

    public HashMap<ComKey, int[]> fkIDCounts = new HashMap<>();
    //public HashMap<ComKey, HashMap<Integer, Integer>> idFreqDis = new HashMap<>();

    public HashMap<String, ArrayList<ComKey>> jointDegreeTable = new HashMap<>(); //key is the table A, values are the tables REFERENCING table A

    public HashMap<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> jointDegrees = new HashMap<>();
    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseJointDegrees = new HashMap<>();
    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeDis = new HashMap<>();

    public HashMap<String, ArrayList<ArrayList<ArrayList<Integer>>>> tupleRVs = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseRVs = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> rvDis = new HashMap<>();

    public HashMap<String, ArrayList<ArrayList<ArrayList<Integer>>>> idKVs = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseKV = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> kvDis = new HashMap<>();

    public void dropKVDis() {
        this.kvDis = null;
    }

    /**
     * This method calculates the counts of each FK IDs
     *
     * @param db
     */
    public void loadFKIDCounts(DB db) {
        for (ComKey ck : db.getComKeys()) {
            String tableName = ck.getReferencingTable();
            int colNum = ck.getReferenceposition();
            int tableID = db.getTableID(tableName);

            String srcTable = ck.getSourceTable();
            int[] idcounts = new int[db.getTableSize(srcTable)];

            for (int i = 0; i < db.getTableSize(tableName); i++) {
                int[] arr = db.getFKValues(tableID, i);
                int id = arr[colNum - 1];
                idcounts[id] += 1;
            }
            fkIDCounts.put(ck, idcounts);
        }
    }

    /**
     * This method calculates the jointTableMapping, The KEY is the source table
     * The VALUES are the referencing tables
     *
     * @param db
     */
    public void processJointDegreeTable(DB db) {
        for (ComKey ck : db.getComKeys()) {
            if (!jointDegreeTable.containsKey(ck.getSourceTable())) {
                jointDegreeTable.put(ck.getSourceTable(), new ArrayList<ComKey>());
            }
            if (!jointDegreeTable.get(ck.getSourceTable()).contains(ck)) {
                jointDegreeTable.get(ck.getSourceTable()).add(ck);
            }
        }
    }

    /**
     * This method calculates the joint-degree for tuples in all tables,
     * JointDegrees is the map which maps the tupleID to Joint-degree
     * reverseJointDegrees maps the Joint-Degree to the tupleIDs having that JD.
     *
     * @param db
     */
    public void calculateJointDegrees(DB db
    ) {
        processJointDegreeTable(db);

        for (Map.Entry<String, ArrayList<ComKey>> entry : jointDegreeTable.entrySet()) {
            ArrayList<ComKey> referencingFKs = entry.getValue();
            HashMap<ArrayList<Integer>, ArrayList<Integer>> jointDegreeIdMap = new HashMap<>();
            String tableName = entry.getKey();
            ArrayList<ArrayList<Integer>> allJointDegrees = new ArrayList<>(db.getTableSize(tableName));

            for (int pkid = 0; pkid < db.getTableSize(tableName); pkid++) {
                ArrayList<Integer> degrees = new ArrayList<>(referencingFKs.size());

                for (int o = 0; o < referencingFKs.size(); o++) {
                    ComKey ck = referencingFKs.get(o);
                    degrees.add(fkIDCounts.get(ck)[pkid]);
                }
                if (!jointDegreeIdMap.containsKey(degrees)) {
                    jointDegreeIdMap.put(degrees, new ArrayList<Integer>());
                }
                jointDegreeIdMap.get(degrees).add(pkid);
                allJointDegrees.add(degrees);
            }

            reverseJointDegrees.put(referencingFKs, jointDegreeIdMap);
            jointDegrees.put(referencingFKs, allJointDegrees);
        }
    }
    /**
     * This method parallely calculates RV Distribution 
     * @param originalDB 
     */
    public void calculateRV(DB originalDB) {
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Map.Entry<String, ArrayList<ComKey>> entry : originalDB.fkRelation.entrySet()) {
            ParaLoadCorr plc = new ParaLoadCorr();
            plc.originalDB = originalDB;
            plc.entry = entry;
            plc.jointDegreeTable = jointDegreeTable;
            plc.originalCoDa = this;

            Thread thr = new Thread(plc);
            threadList.add(thr);
            thr.start();

        }

        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    
    /**
     * This method calculates the KV
     */
    public void calculateKV() {
        for (Map.Entry<String, ArrayList<ArrayList<ArrayList<Integer>>>> rvEntry : tupleRVs.entrySet()) {
            String table = rvEntry.getKey();
            for (Map.Entry<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> jointDegree : jointDegrees.entrySet()) {
                if (table.equals(jointDegree.getKey().get(0).getSourceTable())) {
                    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvMap = new HashMap<>();
                    HashMap<ArrayList<ArrayList<Integer>>, Integer> kvDisMap = new HashMap<>();

                    for (int i = 0; i < jointDegree.getValue().size(); i++) {
                        ArrayList<ArrayList<Integer>> kv = new ArrayList<>();
                        kv.add(jointDegree.getValue().get(i));
                        kv.addAll(rvEntry.getValue().get(i));

                        
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
        tupleRVs = null;

    }
    /*
    public void processIdFrequency() {
        for (Map.Entry<ComKey, int[]> entry : fkIDCounts.entrySet()) {
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
    */
    
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
        fkIDCounts = null;

    }
    
    /*
    public void dropIdFreqDis() {
        idFreqDis = null;
    }*/
}
