/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paraComputation;

import db.structs.DB;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaKVTableGeneration implements Runnable {

    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseKV;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledKVIDs;

    HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap;
    int[][][] scaledRVFKIDs;
    String curTable = "";
    int curTableID;

    String outPath = "";
    String delimiter = "";
    DB originalDB;
    Random random = new Random();
    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> scaledKVMappedtoOriginalKV;
    boolean savedMapFlag = false;
    ArrayList<Integer> oldIDs = new ArrayList<>();

    @Override
    public void run() {
        savedMapFlag = originalDB.getTableNonKeyString(curTableID)[0] != null;

        int loopTimes = 0;

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outPath + "/" + curTable + ".txt")), 100000);

            ArrayList<ArrayList<Integer>> originalClosestKV = new ArrayList<>();
            while (scaledKVIDs.getValue().keySet().size() > 0) {
                ArrayList<ArrayList<ArrayList<Integer>>> removedKVs = new ArrayList<>();
                for (Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> scaledKVIDEntry : scaledKVIDs.getValue().entrySet()) {
                    int frequency = scaledKVIDEntry.getValue().size();
                    if (frequency == 0) {
                        removedKVs.add(scaledKVIDEntry.getKey());
                        continue;
                    }

                    if (savedMapFlag) {
                        originalClosestKV = extractClosestOriginalKV(scaledKVIDEntry, loopTimes);
                        if (originalClosestKV.isEmpty()) {
                            continue;
                        }
                    }

                    printTuples(originalClosestKV, frequency, scaledKVIDEntry, bw);
                    removedKVs.add(scaledKVIDEntry.getKey());
                }
                loopTimes++;
                for (ArrayList<ArrayList<Integer>> removedKV : removedKVs) {
                    scaledKVIDs.getValue().remove(removedKV);
                }
            }
            bw.close();
        } catch (IOException ex) {
            Logger.getLogger(ParaKVTableGeneration.class.getName()).log(Level.SEVERE, null, ex);
        }
        originalReverseKV.remove(scaledKVIDs.getKey());
    }

    /**
     * Initialize parameters
     *
     * @param originalReverseKV
     * @param scaledKVIDentry
     * @param scaledJDIDToRVIDMap
     * @param scaledRVFKIDs
     * @param originalDB
     * @param outPath
     * @param delimiter
     */
    public void setInitials(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledKVMappedtoOriginalKV,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseKV,
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledKVIDentry,
            HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap,
            int[][][] scaledRVFKIDs,
            DB originalDB,
            String outPath,
            String delimiter) {
        this.originalReverseKV = originalReverseKV;
        this.scaledKVIDs = scaledKVIDentry;
        this.scaledJDIDToRVIDMap = scaledJDIDToRVIDMap;
        this.scaledRVFKIDs = scaledRVFKIDs;
        this.originalDB = originalDB;
        this.outPath = outPath;
        this.delimiter = delimiter;
        curTable = scaledKVIDs.getKey();
        curTableID = originalDB.getTableID(curTable);
        this.scaledKVMappedtoOriginalKV = scaledKVMappedtoOriginalKV.get(curTable);

    }

    /**
     *
     * @param scaledKV
     * @param originalSumKVMap
     * @return originalClosestKV
     */
    private ArrayList<ArrayList<Integer>> extractKV(ArrayList<ArrayList<Integer>> scaledKV,
            int loopTimes) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        if (originalReverseKV.get(this.scaledKVIDs.getKey()).containsKey(scaledKV)) {
            result = scaledKV;
        } else if (loopTimes > 1) {
            int size = scaledKVMappedtoOriginalKV.get(scaledKV).size();
            return scaledKVMappedtoOriginalKV.get(scaledKV).get(random.nextInt(size));
        }
        return result;
    }

    /**
     * Print Tuples
     *
     * @param originalClosestKV
     * @param frequency
     * @param scaledKVIDEntry
     * @param bw
     */
    private void printTuples(ArrayList<ArrayList<Integer>> originalClosestKV, int frequency,
            Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> scaledKVIDEntry, BufferedWriter bw
    ) {
        int originalNumberOfIDs = -1;
        if (savedMapFlag) {
            originalNumberOfIDs = originalReverseKV.get(this.scaledKVIDs.getKey()).get(originalClosestKV).size();

            oldIDs.clear();
            for (int i : originalReverseKV.get(this.scaledKVIDs.getKey()).get(originalClosestKV)) {
                oldIDs.add(i);
            }
            Collections.shuffle(oldIDs);
        }
        try {

            for (int i = 0; i < frequency; i++) {
                int pkID = scaledKVIDEntry.getValue().get(i);
                bw.write(pkID+"");
                int rvID = scaledJDIDToRVIDMap.get(curTable).get(pkID);
                for (int fkid : scaledRVFKIDs[curTableID][rvID]) {
                    bw.write(delimiter + fkid);
                }
                if (savedMapFlag) {
                    int originalPKID = oldIDs.get(i % originalNumberOfIDs);
                    if (this.originalDB.tables[curTableID].nonKeys[originalPKID] != null) {
                        bw.write(delimiter + this.originalDB.tables[curTableID].nonKeys[originalPKID]);
                    }
                }
                bw.newLine();
            }

        } catch (IOException ex) {
            Logger.getLogger(ParaKVTableGeneration.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private ArrayList<ArrayList<Integer>> extractClosestOriginalKV(Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> scaledKVIDEntry, int loopTimes) {
        ArrayList<ArrayList<Integer>> originalClosestKV = extractKV(scaledKVIDEntry.getKey(), loopTimes);
        if (originalClosestKV.isEmpty()) {
            return new ArrayList<>();
        }
        if (!originalReverseKV.get(this.scaledKVIDs.getKey()).containsKey(originalClosestKV)) {
            System.err.println("originalClosestKV: " + originalClosestKV);
            int jd = originalClosestKV.get(0).get(0);
            for (ArrayList<ArrayList<Integer>> kv : originalReverseKV.get(this.scaledKVIDs.getKey()).keySet()) {
                if (kv.get(0).get(0).equals(jd)) {
                    System.err.print(" " + kv);
                }
            }
        }

        while (originalReverseKV.get(this.scaledKVIDs.getKey()).get(originalClosestKV).isEmpty()) {
            originalReverseKV.get(this.scaledKVIDs.getKey()).remove(originalClosestKV);
            originalClosestKV = extractKV(scaledKVIDEntry.getKey(), loopTimes);
            if (originalClosestKV.isEmpty()) {
                break;
            }
        }

        return originalClosestKV;
    }
}
