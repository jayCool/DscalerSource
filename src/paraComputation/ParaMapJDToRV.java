/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paraComputation;

import dataStructure.AvaliableStatistics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaMapJDToRV implements Runnable {

    public HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap;
    public HashMap<ArrayList<Integer>, AvaliableStatistics> jointDegreeAvaStats;
    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKentry;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDistribution;

    public double sRatio;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV;
    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVMappedScaledRV = new HashMap<>();

    int[] rvSumValues;
    HashSet<Integer> removedRVSumValues;
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVSumMap = new HashMap<>();
    HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> originalJDMappedToScaledJD;
    ArrayList<ArrayList<Integer>> currentlyUsingOriginalRV = new ArrayList<>();
    ArrayList<Integer> currentlyUsingOriginalJD = new ArrayList<>();
    String curTable;
    boolean saveMap;
    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> scaledKVMappedtoOriginalKV = new HashMap<>();
    
    @Override
    public void run() {
        calculateOriginalSumMap();
       
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable = new HashMap<>();

        HashMap<Integer, Integer> mapJDToRV = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex = new HashMap<>();
        HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex = new HashMap<>();
    
        while (!originalRVSumMap.isEmpty() && !originalJDSumMap.isEmpty()) {
            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry : originalKVDistribution.get(curTable).entrySet()) {
                if (originalRVSumMap.isEmpty() || originalJDSumMap.isEmpty()) {
                    break;
                }
                int expectedFrequency = calculateExpectedFrequency(originalKVentry.getValue());
                ArrayList<ArrayList<Integer>> originalRV = extractOriginalRV(originalKVentry);
                ArrayList<Integer> originalJD = originalKVentry.getKey().get(0);
               
                while (expectedFrequency > 0 && !originalRVSumMap.isEmpty() && !originalJDSumMap.isEmpty()) {

                    ArrayList<ArrayList<Integer>> scaledClosestRV = calculateClosestRV(originalRVSumMap, originalRV);
                    ArrayList<Integer> scaledClosestJD = calculateClosestJD(originalJD, originalJDSumMap);
                    if (scaledClosestJD.isEmpty()) {
                        break;
                    }
                    if (scaledClosestRV.isEmpty()) {
                        break;
                    }
                    boolean matched = false;
                    if (originalJD.equals(scaledClosestJD) && originalRV.equals(scaledClosestRV)) {
                        matched = true;
                    }

                    expectedFrequency = synthesizeOneKV(matched, originalKVentry, originalRVSumMap, originalJDSumMap, expectedFrequency, scaledClosestJD,
                            scaledClosestRV, curTable, rvIDStartingIndex, jdIDStartingIndex, mapJDToRV, kvToIDsWithoutTable);
                
                }

            }
        }
        
        scaledReverseKV.put(curTable, kvToIDsWithoutTable);
        scaledJDIDToRVIDMap.put(curTable, mapJDToRV);
    }

    /**
     * This method randomly returns the JD with the closest key sum.
     *
     * @param scaledJDSumMap
     * @param originalJD
     * @return closestJDBasedOnKeySum
     */
    private ArrayList<Integer> calculateJDFromClosestKeySum(HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap,
            ArrayList<Integer> originalJD) {

        int originalKeySum = calculateJDSum(originalJD);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (scaledJDSumMap.containsKey(i + originalKeySum) && !scaledJDSumMap.get(i + originalKeySum).isEmpty()) {
                int jdKeySum = i + originalKeySum;
                return scaledJDSumMap.get(jdKeySum).get(0);
            }
            if (scaledJDSumMap.containsKey(-i + originalKeySum) && !scaledJDSumMap.get(-i + originalKeySum).isEmpty()) {
                int jdKeySum = -i + originalKeySum;
                return scaledJDSumMap.get(jdKeySum).get(0);
            }
        }
        System.out.println("error in finding closest JD sum");
        return null;
    }

    /**
     *
     * @param jointDegreeSet
     * @return calculatedJDSumMap
     */
    private HashMap<Integer, ArrayList<ArrayList<Integer>>> calculateJDSumMap(Set<ArrayList<Integer>> jointDegreeSet) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> jdSumMap = new HashMap<>();
        for (ArrayList<Integer> jointDegree : jointDegreeSet) {
            int jdSum = 0;
            for (int degree : jointDegree) {
                jdSum += degree;
            }
            if (!jdSumMap.containsKey(Math.abs(jdSum))) {
                jdSumMap.put(Math.abs(jdSum), new ArrayList<ArrayList<Integer>>());
            }
            jdSumMap.get(Math.abs(jdSum)).add(jointDegree);
        }
        return jdSumMap;

    }

    /**
     *
     * @param jointDegree
     * @return sum of the degrees for one JD
     */
    private int calculateJDSum(ArrayList<Integer> jointDegree) {
        int jdSum = 0;
        for (Integer degree : jointDegree) {
            jdSum += degree;
        }
        return jdSum;
    }

    /**
     *
     * @param rv
     * @return sum of degrees for one RV
     */
    private int calculateRVSum(ArrayList<ArrayList<Integer>> rv) {
        int rvSum = 0;
        for (ArrayList<Integer> jointDegree : rv) {
            rvSum += calculateJDSum(jointDegree);
        }
        return rvSum;
    }

    /**
     *
     * @param table
     * @param originalJD
     * @param scaledJDSumMap
     * @return scaledClosestJD
     */
    private ArrayList<Integer> calculateClosestJD(ArrayList<Integer> originalJD, HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap) {
        ArrayList<Integer> scaledClosestJD = new ArrayList<>();

        cleanRemovedJDSums();
        if (jointDegreeAvaStats.containsKey(originalJD)) {
            int jdSum = calculateJDSum(originalJD);
            if (originalJDSumMap.containsKey(jdSum) && originalJDSumMap.get(jdSum).contains(scaledClosestJD)
                    && originalJDMappedToScaledJD.containsKey(originalJD) && originalJDMappedToScaledJD.get(originalJD).contains(originalJD)) {
                scaledClosestJD = originalJD;
                currentlyUsingOriginalJD = originalJD;
            } else {
                return calculateClosestOriginalJDAndScaledJD(originalJD, originalJDMappedToScaledJD);
            }
        } else {
            return calculateClosestOriginalJDAndScaledJD(originalJD, originalJDMappedToScaledJD);
        }

        return scaledClosestJD;

    }

    private void cleanRemovedRVSums() {
        if (removedRVSumValues.size() > 0.1 * rvSumValues.length) {
            int[] tempValues = new int[rvSumValues.length - removedRVSumValues.size()];
            int count = 0;
            for (int i : rvSumValues) {
                if (!removedRVSumValues.contains(i)) {
                    tempValues[count] = i;
                    count++;
                }
            }
            rvSumValues = tempValues;
            removedRVSumValues.clear();
            cleanRVMapping(originalRVMappedScaledRV);
            cleanOriginalRVSumMapping(originalRVSumMap);
        }
    }

    private void cleanRemovedJDSums() {
        if (removedJDSumValues.size() > 0.1 * jdSumValues.length) {
            int[] tempValues = new int[jdSumValues.length - removedJDSumValues.size()];
            int count = 0;
            for (int i : jdSumValues) {
                if (!removedJDSumValues.contains(i)) {
                    tempValues[count] = i;
                    count++;
                }
            }
            jdSumValues = tempValues;
            removedJDSumValues.clear();
            cleanJDMapping(originalJDMappedToScaledJD);
            cleanOriginalJDSumMapping(originalJDSumMap);
        }
    }

    /**
     *
     * originalRVSumMap scaledRVSumMap
     *
     * @param originalRV
     * @return scaledClosestRV
     */
    private ArrayList<ArrayList<Integer>> calculateClosestRV(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVSumMap,
            ArrayList<ArrayList<Integer>> originalRV) {
        ArrayList<ArrayList<Integer>> scaledClosestRV = new ArrayList<>();

        cleanRemovedRVSums();

        if (scaledRVPKentry.getValue().containsKey(originalRV)) {
            int rvKeySum = calculateRVSum(scaledClosestRV);

            if (originalRVSumMap.containsKey(rvKeySum) && originalRVSumMap.get(rvKeySum).contains(scaledClosestRV)
                    && originalRVMappedScaledRV.containsKey(originalRV) && originalRVMappedScaledRV.get(originalRV).contains(originalRV)) {
                scaledClosestRV = originalRV;
                currentlyUsingOriginalRV = originalRV;
            } else {
                return calculateClosestOriginalAndScaledRV(originalRV, originalRVMappedScaledRV);

            }
        } else {
            return calculateClosestOriginalAndScaledRV(originalRV, originalRVMappedScaledRV);
        }
        return scaledClosestRV;

    }

    /**
     *
     * @param originalFrequency
     * @return expected scaledFrequency
     */
    private int calculateExpectedFrequency(Integer originalFrequency) {
        int scaledFrequency = (int) (originalFrequency * this.sRatio);
        if (Math.random() < (originalFrequency * this.sRatio - (int) (originalFrequency * this.sRatio))) {
            scaledFrequency += 1;
        }
        return scaledFrequency;
    }

    /**
     * Extract the original RV from KV Entry
     *
     * @param originalKVentry
     * @return currentlyUsingOriginalRV
     */
    private ArrayList<ArrayList<Integer>> extractOriginalRV(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry) {
        ArrayList<ArrayList<Integer>> originalRV = new ArrayList<>();
        for (int i = 1; i < originalKVentry.getKey().size(); i++) {
            originalRV.add(originalKVentry.getKey().get(i));
        }
        return originalRV;
    }

    public void setInitials(boolean saveMap, HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledKVMappedtoOriginalKV, 
            HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap, double sRatio,
            HashMap<ArrayList<Integer>, AvaliableStatistics> jointDegreeAvaStats,
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKentry,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDistribution,
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVMappedScaledRV,
            HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> originalJDMappedToScaledJD
    ) {
         curTable = scaledRVPKentry.getKey();
        this.saveMap = saveMap;
        scaledKVMappedtoOriginalKV.put(curTable, this.originalRVMappedScaledRV);
        this.scaledJDIDToRVIDMap = scaledJDIDToRVIDMap;
        this.sRatio = sRatio;
        this.jointDegreeAvaStats = jointDegreeAvaStats;
        this.scaledRVPKentry = scaledRVPKentry;
        this.scaledReverseKV = scaledReverseKV;
        this.originalKVDistribution = originalKVDistribution;
        this.originalRVMappedScaledRV = originalRVMappedScaledRV;
        this.originalJDMappedToScaledJD = originalJDMappedToScaledJD;
    }

    /**
     * This function initializes the starting index to retrieve the IDs, This
     * function mainly helps the efficiency for retrieving IDs.
     *
     * @param rvIDStartingIndex
     * @param scaledClosestRV
     * @param jdIDStartingIndex
     * @param scaledClosestJD
     */
    private void initializeIDStartingIndexes(HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex, ArrayList<ArrayList<Integer>> scaledClosestRV, HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex, ArrayList<Integer> scaledClosestJD) {
        if (!rvIDStartingIndex.containsKey(scaledClosestRV)) {
            rvIDStartingIndex.put(scaledClosestRV, 0);
        }

        if (!jdIDStartingIndex.containsKey(scaledClosestJD)) {
            jdIDStartingIndex.put(scaledClosestJD, 0);
        }
    }

    /**
     *
     * @param scaledRVIDs
     * @param rvIDStartingIndex
     * @param scaledClosestRV
     * @param scaledJDIDs
     * @param jdIDStartingIndex
     * @param scaledClosestJD
     * @param expectedFrequency
     * @return incrementalFrequency
     */
    private int calculateIncrementalFrequency(ArrayList<Integer> scaledRVIDs, HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex, ArrayList<ArrayList<Integer>> scaledClosestRV, int[] scaledJDIDs, HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex, ArrayList<Integer> scaledClosestJD, int expectedFrequency) {
        int incrementalFrequency = Math.min(scaledRVIDs.size() - rvIDStartingIndex.get(scaledClosestRV),
                scaledJDIDs.length - jdIDStartingIndex.get(scaledClosestJD));

        incrementalFrequency = Math.min(incrementalFrequency, expectedFrequency);
        return incrementalFrequency;
    }

    /**
     *
     * @param scaledClosestJD
     * @param scaledClosestRV
     * @return scaledKV
     */
    private ArrayList<ArrayList<Integer>> formScaledKV(ArrayList<Integer> scaledClosestJD,
            ArrayList<ArrayList<Integer>> scaledClosestRV,
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable,
            Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry, boolean matched) {
        ArrayList<ArrayList<Integer>> scaledKV = new ArrayList<>();
        if (matched) {
            scaledKV = originalKVentry.getKey();
        } else {
            scaledKV.add(scaledClosestJD);
            scaledKV.addAll(scaledClosestRV);
        }

        if (!kvToIDsWithoutTable.containsKey(scaledKV)) {
            kvToIDsWithoutTable.put(scaledKV, new ArrayList<Integer>());
        }

        return scaledKV;
    }

    private void updateIDMaps(int incrementalFrequency, HashMap<Integer, Integer> mapJDToRV,
            int[] scaledJDIDs, ArrayList<Integer> scaledRVIDs, int rvIndex, ArrayList<ArrayList<Integer>> scaledKV,
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable, int jdIndex) {
        for (int i = 0; i < incrementalFrequency; i++) {
            mapJDToRV.put(scaledJDIDs[i + jdIndex], scaledRVIDs.get(i + rvIndex));
            kvToIDsWithoutTable.get(scaledKV).add(scaledJDIDs[i + jdIndex]);
        }
    }

    private void updateIDStartingIndex(ArrayList<Integer> scaledClosestJD, int incrementalFrequency, int jdIndex, ArrayList<ArrayList<Integer>> scaledClosestRV, int rvIndex, HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex, HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex) {
        rvIDStartingIndex.put(scaledClosestRV, incrementalFrequency + rvIndex);
        jdIDStartingIndex.put(scaledClosestJD, incrementalFrequency + jdIndex);
    }

    private void cleanRVIDsIfNecessary(ArrayList<Integer> scaledRVIDs, int incrementalFrequency, int rvIndex,
            HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVSumMap, ArrayList<ArrayList<Integer>> scaledClosestRV) {

        if (scaledRVIDs.size() == (incrementalFrequency + rvIndex)) {
            if (originalRVMappedScaledRV.get(currentlyUsingOriginalRV).size() == 1) {
                originalRVMappedScaledRV.remove(currentlyUsingOriginalRV);
                int originalRVSum = calculateRVSum(currentlyUsingOriginalRV);
                originalRVSumMap.get(originalRVSum).remove(currentlyUsingOriginalRV);

            } else {
                originalRVMappedScaledRV.get(currentlyUsingOriginalRV).remove(scaledClosestRV);
            }

        }

    }

    private void cleanJDIDsIfNecessary(int[] scaledJDIDs, int incrementalFrequency, int jdIndex,
            HashMap<Integer, ArrayList<ArrayList<Integer>>> originalJDSumMap, ArrayList<Integer> scaledClosestJD) {
        if (scaledJDIDs.length == (incrementalFrequency + jdIndex)) {
            if (originalJDMappedToScaledJD.get(currentlyUsingOriginalJD).size() == 1) {
                originalJDMappedToScaledJD.remove(currentlyUsingOriginalJD);
                int originalJDSum = calculateJDSum(currentlyUsingOriginalJD);
                originalJDSumMap.get(originalJDSum).remove(currentlyUsingOriginalJD);

            } else {
                originalJDMappedToScaledJD.get(currentlyUsingOriginalJD).remove(scaledClosestJD);
            }
        }
    }

    private int synthesizeOneKV(boolean matched, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry,
            HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVSumMap,
            HashMap<Integer, ArrayList<ArrayList<Integer>>> originalJDSumMap, int expectedFrequency,
            ArrayList<Integer> scaledClosestJD, ArrayList<ArrayList<Integer>> scaledClosestRV, String table,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex,
            HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex, HashMap<Integer, Integer> mapJDToRV,
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable) {
        ArrayList<Integer> scaledRVIDs = scaledRVPKentry.getValue().get(scaledClosestRV);
        int[] scaledJDIDs = jointDegreeAvaStats.get(scaledClosestJD).ids;

        initializeIDStartingIndexes(rvIDStartingIndex, scaledClosestRV, jdIDStartingIndex, scaledClosestJD);
        int incrementalFrequency = calculateIncrementalFrequency(scaledRVIDs, rvIDStartingIndex, scaledClosestRV, scaledJDIDs, jdIDStartingIndex, scaledClosestJD, expectedFrequency);

        ArrayList<ArrayList<Integer>> scaledKV = formScaledKV(scaledClosestJD, scaledClosestRV, kvToIDsWithoutTable, originalKVentry, matched);

        int rvIndex = rvIDStartingIndex.get(scaledClosestRV);
        int jdIndex = jdIDStartingIndex.get(scaledClosestJD);

        updateIDMaps(incrementalFrequency, mapJDToRV, scaledJDIDs, scaledRVIDs, rvIndex, scaledKV, kvToIDsWithoutTable, jdIndex);
        expectedFrequency -= incrementalFrequency;
        if (incrementalFrequency!=0 && saveMap){
            if (!scaledKVMappedtoOriginalKV.containsKey(scaledKV)){
                scaledKVMappedtoOriginalKV.put(scaledKV, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            if (!scaledKVMappedtoOriginalKV.get(scaledKV).contains(originalKVentry.getKey())){
                scaledKVMappedtoOriginalKV.get(scaledKV).add(originalKVentry.getKey());
            }
        }
        updateIDStartingIndex(scaledClosestJD, incrementalFrequency, jdIndex, scaledClosestRV, rvIndex, rvIDStartingIndex, jdIDStartingIndex);

        cleanRVIDsIfNecessary(scaledRVIDs, incrementalFrequency, rvIndex, originalRVSumMap, scaledClosestRV);
        cleanJDIDsIfNecessary(scaledJDIDs, incrementalFrequency, jdIndex, originalJDSumMap, scaledClosestJD);
        return expectedFrequency;
    }

    HashMap<Integer, ArrayList<ArrayList<Integer>>> originalJDSumMap = new HashMap<>();
    int[] jdSumValues;
    HashSet<Integer> removedJDSumValues = new HashSet<>();

    private void calculateOriginalSumMap() {

        for (ArrayList<ArrayList<Integer>> originalRV : originalRVMappedScaledRV.keySet()) {
            int rvSum = calculateRVSum(originalRV);
            if (!originalRVSumMap.containsKey(rvSum)) {
                originalRVSumMap.put(rvSum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            originalRVSumMap.get(rvSum).add(originalRV);
        }
        rvSumValues = new int[originalRVSumMap.size()];
        removedRVSumValues = new HashSet<Integer>();
        int count = 0;
        for (int rvSum : originalRVSumMap.keySet()) {
            rvSumValues[count] = rvSum;
            count++;
        }

        for (ArrayList<Integer> originalJD : originalJDMappedToScaledJD.keySet()) {
            int jdSum = calculateJDSum(originalJD);
            if (!originalJDSumMap.containsKey(jdSum)) {
                originalJDSumMap.put(jdSum, new ArrayList<ArrayList<Integer>>());
            }
            originalJDSumMap.get(jdSum).add(originalJD);
        }

        jdSumValues = new int[originalJDSumMap.size()];
        removedJDSumValues = new HashSet<>();

        count = 0;
        for (int jdSum : originalJDSumMap.keySet()) {
            jdSumValues[count] = jdSum;
            count++;
        }

    }

    private ArrayList<ArrayList<Integer>> calculateOriginalRV(ArrayList<ArrayList<Integer>> originalRV) {
        if (originalRVMappedScaledRV.containsKey(originalRV) && !originalRVMappedScaledRV.get(originalRV).isEmpty()) {
            return originalRV;
        } else {
            int originalSum = calculateRVSum(originalRV);
            if (originalRVSumMap.containsKey(originalSum)) {
                originalRVSumMap.get(originalSum).remove(originalRV);
            }
            originalRVMappedScaledRV.remove(originalRV);
            while (!originalRVSumMap.isEmpty()) {
                int matchingIndex = Math.abs(Arrays.binarySearch(rvSumValues, originalSum));
                for (int i = 0; i < rvSumValues.length; i++) {
                    for (int j = -1; j <= 1; j += 2) {
                        int newMatchingIndex = matchingIndex + i * j;
                        if (newMatchingIndex >= rvSumValues.length || newMatchingIndex < 0) {
                            continue;
                        }
                        int newRVSum = rvSumValues[newMatchingIndex];
                        if (originalRVSumMap.containsKey(newRVSum) && originalRVSumMap.get(newRVSum).size() == 0) {
                            originalRVSumMap.remove(newRVSum);
                            removedRVSumValues.add(newRVSum);
                        } else if (originalRVSumMap.containsKey(newRVSum)) {
                            int minIndex = calculateMinIndexForRV(originalRVSumMap.get(newRVSum), originalRVMappedScaledRV, originalRV, newRVSum);
                            if (minIndex != -1) {
                                return originalRVSumMap.get(newRVSum).get(minIndex);
                            } else {
                                originalRVSumMap.remove(newRVSum);
                                removedRVSumValues.add(newRVSum);
                            }
                        }
                    }
                }
            }
        }
        return new ArrayList<>();
    }

    private int calculateMinIndexForRV(ArrayList<ArrayList<ArrayList<Integer>>> rvs, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVMappedScaledRV, ArrayList<ArrayList<Integer>> originalRV, int newRVSum) {
        int minIndex = -1;
        int minDistance = Integer.MAX_VALUE;

        if (rvs.size() == 1) {
            if (!originalRVMappedScaledRV.containsKey(rvs.get(0)) || originalRVMappedScaledRV.get(rvs.get(0)).isEmpty()) {
                originalRVMappedScaledRV.remove(rvs.get(0));
                return -1;
            }
            return 0;
        }
        for (int i = 0; i < rvs.size(); i++) {
            ArrayList<ArrayList<Integer>> rv = rvs.get(i);
            if (!originalRVMappedScaledRV.containsKey(rv) || originalRVMappedScaledRV.get(rv).isEmpty()) {
                originalRVMappedScaledRV.remove(rv);
                continue;
            }
            int distance = calculateRVDistance(rv, originalRV);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        return minIndex;
    }

    private int calculateRVDistance(ArrayList<ArrayList<Integer>> rv, ArrayList<ArrayList<Integer>> originalRV) {
        int distance = 0;
        for (int i = 0; i < rv.size(); i++) {
            distance += calculateJDDistance(rv.get(i), originalRV.get(i));
        }
        return distance;
    }

    private int calculateJDDistance(ArrayList<Integer> jd1, ArrayList<Integer> jd2) {
        int distance = 0;
        for (int i = 0; i < jd1.size(); i++) {
            distance += Math.abs(jd1.get(i) - jd2.get(i));
        }
        return distance;
    }

    private void cleanRVMapping(HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVMappedScaledRV) {
        ArrayList<ArrayList<ArrayList<Integer>>> removedRVs = new ArrayList<>();
        for (Entry<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> entry : originalRVMappedScaledRV.entrySet()) {
            if (entry.getValue().size() == 0) {
                removedRVs.add(entry.getKey());
            }
        }
        for (ArrayList<ArrayList<Integer>> removedRV : removedRVs) {
            originalRVMappedScaledRV.remove(removedRV);
        }
    }

    private void cleanJDMapping(HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> originalJDMappedToScaledJD) {
        ArrayList<ArrayList<Integer>> removedJDs = new ArrayList<>();
        for (Entry<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> entry : originalJDMappedToScaledJD.entrySet()) {
            if (entry.getValue().isEmpty()) {
                removedJDs.add(entry.getKey());
            }
        }
        for (ArrayList<Integer> removedJD : removedJDs) {
            originalJDMappedToScaledJD.remove(removedJD);
        }
    }

    private void cleanOriginalRVSumMapping(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVSumMap) {
        ArrayList<Integer> removedSums = new ArrayList<>();
        for (int k : originalRVSumMap.keySet()) {
            if (originalRVSumMap.get(k).isEmpty()) {
                removedSums.add(k);
            }
        }
        for (int k : removedSums) {
            originalRVSumMap.remove(k);
        }
    }

    private void cleanOriginalJDSumMapping(HashMap<Integer, ArrayList<ArrayList<Integer>>> originalJDSumMap) {
        ArrayList<Integer> removedSums = new ArrayList<>();
        for (int k : originalJDSumMap.keySet()) {
            if (originalJDSumMap.get(k).isEmpty()) {
                removedSums.add(k);
            }
        }
        for (int k : removedSums) {
            originalJDSumMap.remove(k);
        }
    }

    private ArrayList<Integer> calculateClosestOriginalJDAndScaledJD(ArrayList<Integer> originalJD, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> originalJDMappedToScaledJD) {
        ArrayList<Integer> calculatedOriginalJD = calculateOriginalJD(originalJD);
        currentlyUsingOriginalJD = calculatedOriginalJD;
        if (calculatedOriginalJD.isEmpty()) {
            return calculatedOriginalJD;
        }

        return originalJDMappedToScaledJD.get(calculatedOriginalJD).get(0);
    }

    private ArrayList<ArrayList<Integer>> calculateClosestOriginalAndScaledRV(ArrayList<ArrayList<Integer>> originalRV, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVMappedScaledRV) {
        ArrayList<ArrayList<Integer>> calculatedOriginalRV = calculateOriginalRV(originalRV);
        currentlyUsingOriginalRV = calculatedOriginalRV;
        if (calculatedOriginalRV.isEmpty()) {
            return calculatedOriginalRV;
        }
        return originalRVMappedScaledRV.get(calculatedOriginalRV).get(0);
    }

    private ArrayList<Integer> calculateOriginalJD(ArrayList<Integer> originalJD) {
        if (originalJDMappedToScaledJD.containsKey(originalJD) && !originalJDMappedToScaledJD.get(originalJD).isEmpty()) {
            return originalJD;
        } else {
            int originalSum = calculateJDSum(originalJD);
            if (originalJDSumMap.containsKey(originalSum)) {
                originalJDSumMap.get(originalSum).remove(originalJD);
            }
            originalJDMappedToScaledJD.remove(originalJD);
            while (!originalJDSumMap.isEmpty()) {
                int matchingIndex = Math.abs(Arrays.binarySearch(jdSumValues, originalSum));
                for (int i = 0; i < jdSumValues.length; i++) {
                    for (int j = -1; j <= 1; j += 2) {
                        int newMatchingIndex = matchingIndex + i * j;
                        if (newMatchingIndex >= jdSumValues.length || newMatchingIndex < 0) {
                            continue;
                        }
                        int newJDSum = jdSumValues[newMatchingIndex];
                        if (originalJDSumMap.containsKey(newJDSum) && originalJDSumMap.get(newJDSum).size() == 0) {
                            originalJDSumMap.remove(newJDSum);
                            removedJDSumValues.add(newJDSum);
                        } else if (originalJDSumMap.containsKey(newJDSum)) {
                            int minIndex = calculateMinIndexForJD(originalJDSumMap.get(newJDSum), originalJDMappedToScaledJD, originalJD, newJDSum);
                            if (minIndex != -1) {
                                return originalJDSumMap.get(newJDSum).get(minIndex);
                            } else {
                                originalJDSumMap.remove(newJDSum);
                                removedJDSumValues.add(newJDSum);
                            }
                        }
                    }
                }
            }
        }
        return new ArrayList<>();
    }

    private int calculateMinIndexForJD(ArrayList<ArrayList<Integer>> jds, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> originalJDMappedToScaledJD,
            ArrayList<Integer> originalJD, int newJDSum) {
        int minIndex = -1;
        int minDistance = Integer.MAX_VALUE;

        if (jds.size() == 1) {
            if (!originalJDMappedToScaledJD.containsKey(jds.get(0)) || originalJDMappedToScaledJD.get(jds.get(0)).isEmpty()) {
                originalJDMappedToScaledJD.remove(jds.get(0));
                return -1;
            }
            return 0;
        }
        for (int i = 0; i < jds.size(); i++) {
            ArrayList<Integer> jd = jds.get(i);
            if (!originalJDMappedToScaledJD.containsKey(jd) || originalJDMappedToScaledJD.get(jd).isEmpty()) {
                originalJDMappedToScaledJD.remove(jd);
                continue;
            }
            int distance = calculateJDDistance(jd, originalJD);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        return minIndex;
    }
}
