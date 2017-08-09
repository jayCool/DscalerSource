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

    @Override
    public void run() {
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalSumKVMap = calculateKVSumMap(originalReverseKV.get(scaledKVIDs.getKey()).keySet());

        curTable = scaledKVIDs.getKey();
        curTableID = originalDB.getTableID(curTable);
        int loopTimes = 0;

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outPath + "/" + curTable + ".txt")), 100000);
            System.err.println("id size: " + scaledKVIDs.getValue().size());
            while (scaledKVIDs.getValue().keySet().size() > 0) {
                ArrayList<ArrayList<ArrayList<Integer>>> removedKVs = new ArrayList<>();
                for (Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> scaledKVIDEntry : scaledKVIDs.getValue().entrySet()) {
                    int frequency = scaledKVIDEntry.getValue().size();

                    ArrayList<ArrayList<Integer>> originalClosestKV = extractKV(scaledKVIDEntry.getKey(), originalSumKVMap, loopTimes);
                    if (originalClosestKV.isEmpty()|| frequency == 0) {
                        if (frequency == 0) {
                            removedKVs.add(scaledKVIDEntry.getKey());
                        }
                        continue;
                    }
                    if (!originalReverseKV.get(this.scaledKVIDs.getKey()).containsKey(originalClosestKV)){
                        System.err.println("originalClosestKV: " + originalClosestKV);
                        int jd = originalClosestKV.get(0).get(0);
                        for (ArrayList<ArrayList<Integer>> kv: originalReverseKV.get(this.scaledKVIDs.getKey()).keySet()){
                            if (kv.get(0).get(0).equals(jd)){
                                System.err.print(" " + kv);
                            }
                        }
                        //System.err.println("originalReverseKV.get(this.scaledKVIDs.getKey())" + originalReverseKV.get(this.scaledKVIDs.getKey()).keySet());
                    }
                    while (originalReverseKV.get(this.scaledKVIDs.getKey()).get(originalClosestKV).isEmpty()) {
                        cleanZeroIDKV(originalClosestKV, originalSumKVMap, scaledKVIDEntry);
                        originalClosestKV = extractKV(scaledKVIDEntry.getKey(), originalSumKVMap, loopTimes);
                        if (originalClosestKV.isEmpty()){
                            break;
                        }
                    }
                    
                    if (originalClosestKV.isEmpty()){
                        continue;
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
     *
     * @param kvSet
     * @return sumKVMap
     */
    private HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> calculateKVSumMap(Set<ArrayList<ArrayList<Integer>>> kvSet) {
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> sumKVMap = new HashMap<>();
        for (ArrayList<ArrayList<Integer>> kv : kvSet) {
            int sum = 0;
            for (ArrayList<Integer> jointDegree : kv) {
                for (int degree : jointDegree) {
                    sum += degree;
                }
            }
            if (!sumKVMap.containsKey(sum)) {
                sumKVMap.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            sumKVMap.get(sum).add(kv);
        }
        return sumKVMap;
    }

    /**
     *
     * @param scaledKV
     * @param originalSumKVMap
     * @return originalClosestKV
     */
    private ArrayList<ArrayList<Integer>> calculateClosestKVBasedOnSum(ArrayList<ArrayList<Integer>> scaledKV, HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalSumKVMap) {
        int scaledSum = calculateKVSum(scaledKV);
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {

            if (originalSumKVMap.containsKey(scaledSum + i)) {
                result = extractKVWithSum(scaledSum + i, originalSumKVMap);
            }

            if (!result.isEmpty()) {
                return result;
            }

            if (originalSumKVMap.containsKey(scaledSum - i)) {
                result = extractKVWithSum(scaledSum - i, originalSumKVMap);
            }

            if (!result.isEmpty()) {
                return result;
            }
        }
        return result;
    }

    /**
     *
     * @param kv
     * @return sum of the KV
     */
    private int calculateKVSum(ArrayList<ArrayList<Integer>> kv) {
        int sum = 0;
        for (ArrayList<Integer> jointDegree : kv) {
            for (int i : jointDegree) {
                sum += i;
            }
        }
        return sum;
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
    public void setInitials(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseKV,
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
    }

    /**
     *
     * @param scaledKV
     * @param originalSumKVMap
     * @return originalClosestKV
     */
    private ArrayList<ArrayList<Integer>> extractKV(ArrayList<ArrayList<Integer>> scaledKV,
            HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalSumKVMap, int loopTimes) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        if (originalReverseKV.get(this.scaledKVIDs.getKey()).containsKey(scaledKV)) {
            result = scaledKV;
        } else if (loopTimes > 1) {
            result = calculateClosestKVBasedOnSum(scaledKV, originalSumKVMap);
        }
        return result;
    }

    /**
     *
     * @param sum
     * @param originalSumKVMap
     * @return KV with the specific sum
     */
    private ArrayList<ArrayList<Integer>> extractKVWithSum(int sum, HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalSumKVMap) {
        int size = originalSumKVMap.get(sum).size();
        if (size == 0) {
            originalSumKVMap.remove(sum);
        } else {
            return originalSumKVMap.get(sum).get(random.nextInt(size));
        }
        return new ArrayList<ArrayList<Integer>>();
    }

    /**
     * CleanZeroIDMap
     *
     * @param originalClosestKV
     * @param originalSumKVMap
     * @param concurrentScaledKVIDEntry
     */
    private void cleanZeroIDKV(ArrayList<ArrayList<Integer>> originalClosestKV, HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalSumKVMap, Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> concurrentScaledKVIDEntry) {
        originalReverseKV.get(this.scaledKVIDs.getKey()).remove(originalClosestKV);
        int originalKVSum = calculateKVSum(originalClosestKV);
        originalSumKVMap.get(originalKVSum).remove(originalClosestKV);
        if (originalSumKVMap.get(originalKVSum).isEmpty()) {
            originalSumKVMap.remove(originalKVSum);
        }
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
        int originalNumberOfIDs = originalReverseKV.get(this.scaledKVIDs.getKey()).get(originalClosestKV).size();

        ArrayList<Integer> oldIDs = new ArrayList<>();
        for (int i : originalReverseKV.get(this.scaledKVIDs.getKey()).get(originalClosestKV)) {
            oldIDs.add(i);
        }
        Collections.shuffle(oldIDs);
        try {

            for (int i = 0; i < frequency; i++) {
                int pkID = scaledKVIDEntry.getValue().get(i);
                bw.write(pkID);
                //  System.out.println("scaledJDIDToRVIDMap.get(curTable).get(pkID): " + scaledJDIDToRVIDMap.get(curTable).get(pkID));
                int rvID = scaledJDIDToRVIDMap.get(curTable).get(pkID);
                // System.err.println("sss" + "\t" + scaledRVFKIDs);
                for (int fkid : scaledRVFKIDs[curTableID][rvID]) {
                    bw.write(delimiter + fkid);
                }
                int originalPKID = oldIDs.get(i % originalNumberOfIDs);
                if (this.originalDB.tables[curTableID].nonKeys[originalPKID] != null) {
                    bw.write(delimiter + this.originalDB.tables[curTableID].nonKeys[originalPKID]);
                }
                bw.newLine();
            }

        } catch (IOException ex) {
            Logger.getLogger(ParaKVTableGeneration.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
