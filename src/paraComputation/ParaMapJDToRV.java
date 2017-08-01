/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paraComputation;

import dataStructure.AvaliableStatistics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaMapJDToRV implements Runnable {

    public HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap;
    public HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats;
    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKentry;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDistribution;

    public double sRatio;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV;

    @Override
    public void run() {
        String table = scaledRVPKentry.getKey();
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable = new HashMap<>();
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap = calculateRVSumMap(scaledRVPKentry.getValue().keySet());
        HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap = calculateJDSumMap(jointDegreeAvaStats.get(table).keySet());

        HashMap<Integer, Integer> mapJDToRV = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex = new HashMap<>();
        HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex = new HashMap<>();

        while (scaledRVSumMap.isEmpty() && scaledJDSumMap.isEmpty()) {
            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry : originalKVDistribution.get(table).entrySet()) {
                if (scaledRVSumMap.isEmpty() || scaledJDSumMap.isEmpty()) {
                    break;
                }
                int expectedFrequency = calculateExpectedFrequency(originalKVentry.getValue());
                ArrayList<ArrayList<Integer>> originalRV = extractOriginalRV(originalKVentry);
                ArrayList<Integer> originalJD = originalKVentry.getKey().get(0);

                while (expectedFrequency > 0 && !scaledRVSumMap.isEmpty() && !scaledJDSumMap.isEmpty()) {
                    ArrayList<ArrayList<Integer>> scaledClosestRV = calculateClosestRV(scaledRVSumMap, originalRV);
                    ArrayList<Integer> scaledClosestJD = calculateClosestJD(table, originalJD, scaledJDSumMap);

                    expectedFrequency = synthesizeOneKV(scaledRVSumMap, scaledJDSumMap, expectedFrequency, scaledClosestJD,
                            scaledClosestRV, table, rvIDStartingIndex, jdIDStartingIndex, mapJDToRV, kvToIDsWithoutTable);

                }

            }
        }

        scaledReverseKV.put(table, kvToIDsWithoutTable);
        scaledJDIDToRVIDMap.put(table, mapJDToRV);
    }

    /**
     * Randomly returns the RV with the closest key sum
     *
     * @param scaledRVSumMap
     * @param originalRV
     * @return closestRVWithKeySum
     */
    private ArrayList<ArrayList<Integer>> calculateRVFromClosestKeySum(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap,
            ArrayList<ArrayList<Integer>> originalRV) {
        int originalRVSum = calculateRVSum(originalRV);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (scaledRVSumMap.containsKey(i + originalRVSum) && !scaledRVSumMap.get(i + originalRVSum).isEmpty()) {
                int rvKeySum = i + originalRVSum;
                return scaledRVSumMap.get(rvKeySum).get(0);
            }
            if (scaledRVSumMap.containsKey(-i + originalRVSum) && !scaledRVSumMap.get(-i + originalRVSum).isEmpty()) {
                int rvKeySum = -i + originalRVSum;
                return scaledRVSumMap.get(rvKeySum).get(0);
            }

        }
        System.out.println("error in finding RV");
        return null;
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
     * @param rvSet
     * @return rvSumMap
     */
    private HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> calculateRVSumMap(Set<ArrayList<ArrayList<Integer>>> rvSet) {
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> rvSumMap = new HashMap<>();
        for (ArrayList<ArrayList<Integer>> rv : rvSet) {
            int rvSum = calculateRVSum(rv);

            if (!rvSumMap.containsKey(Math.abs(rvSum))) {
                rvSumMap.put(Math.abs(rvSum), new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            rvSumMap.get(Math.abs(rvSum)).add(rv);
        }
        return rvSumMap;
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
            for (int degree : jointDegree) {
                rvSum += degree;
            }
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
    private ArrayList<Integer> calculateClosestJD(String table, ArrayList<Integer> originalJD,
            HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap) {

        ArrayList<Integer> scaledClosestJD = new ArrayList<>();
        if (jointDegreeAvaStats.get(table).keySet().contains(originalJD)) {
            scaledClosestJD = originalJD;
            int jdKeySum = calculateJDSum(scaledClosestJD);

            if (!scaledJDSumMap.containsKey(jdKeySum) || !scaledJDSumMap.get(jdKeySum).contains(scaledClosestJD)) {
                scaledClosestJD = calculateJDFromClosestKeySum(scaledJDSumMap, originalJD);
            }
        } else {
            scaledClosestJD = calculateJDFromClosestKeySum(scaledJDSumMap, originalJD);
        }
        return scaledClosestJD;
    }

    /**
     *
     * @param scaledRVSumMap
     * @param originalRV
     * @return scaledClosestRV
     */
    private ArrayList<ArrayList<Integer>> calculateClosestRV(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap,
            ArrayList<ArrayList<Integer>> originalRV) {
        ArrayList<ArrayList<Integer>> scaledClosestRV = new ArrayList<>();
        if (scaledRVPKentry.getValue().keySet().contains(originalRV)) {
            scaledClosestRV = originalRV;
            int rvKeySum = calculateRVSum(scaledClosestRV);
            if (!scaledRVSumMap.containsKey(rvKeySum) || !scaledRVSumMap.get(rvKeySum).contains(scaledClosestRV)) {
                scaledClosestRV = calculateRVFromClosestKeySum(scaledRVSumMap, originalRV);
            }
        } else {
            scaledClosestRV = calculateRVFromClosestKeySum(scaledRVSumMap, originalRV);
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
     * @return originalRV
     */
    private ArrayList<ArrayList<Integer>> extractOriginalRV(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry) {
        ArrayList<ArrayList<Integer>> originalRV = new ArrayList<>();
        for (int i = 1; i < originalKVentry.getKey().size(); i++) {
            originalRV.add(originalKVentry.getKey().get(i));
        }
        return originalRV;
    }

    public void setInitials(HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap, double sRatio,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKentry,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDistribution) {
        this.scaledJDIDToRVIDMap = scaledJDIDToRVIDMap;
        this.sRatio = sRatio;
        this.jointDegreeAvaStats = jointDegreeAvaStats;
        this.scaledRVPKentry = scaledRVPKentry;
        this.scaledReverseKV = scaledReverseKV;
        this.originalKVDistribution = originalKVDistribution;
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
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable) {
        ArrayList<ArrayList<Integer>> scaledKV = new ArrayList<>();
        scaledKV.add(scaledClosestJD);
        scaledKV.addAll(scaledClosestRV);
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

    private void cleanRVIDsIfNecessary(ArrayList<Integer> scaledRVIDs, int incrementalFrequency, int rvIndex, HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap, ArrayList<ArrayList<Integer>> scaledClosestRV) {

        if (scaledRVIDs.size() == (incrementalFrequency + rvIndex)) {
            int rvKeySum = calculateRVSum(scaledClosestRV);
            scaledRVSumMap.get(rvKeySum).remove(scaledClosestRV);
            if (scaledRVSumMap.get(rvKeySum).isEmpty()) {
                scaledRVSumMap.remove(rvKeySum);
            }
        }
    }

    private void cleanJDIDsIfNecessary(int[] scaledJDIDs, int incrementalFrequency, int jdIndex, HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap, ArrayList<Integer> scaledClosestJD) {
        if (scaledJDIDs.length == (incrementalFrequency + jdIndex)) {
            int jdKeySum = calculateJDSum(scaledClosestJD);
            scaledJDSumMap.get(jdKeySum).remove(scaledClosestJD);
            if (scaledJDSumMap.get(jdKeySum).isEmpty()) {
                scaledJDSumMap.remove(jdKeySum);
            }
        }
    }

    private int synthesizeOneKV(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap, 
            HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap, int expectedFrequency,
            ArrayList<Integer> scaledClosestJD, ArrayList<ArrayList<Integer>> scaledClosestRV, String table, 
            HashMap<ArrayList<ArrayList<Integer>>, Integer> rvIDStartingIndex, 
            HashMap<ArrayList<Integer>, Integer> jdIDStartingIndex, HashMap<Integer, Integer> mapJDToRV, 
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> kvToIDsWithoutTable) {
        ArrayList<Integer> scaledRVIDs = scaledRVPKentry.getValue().get(scaledClosestRV);
        int[] scaledJDIDs = jointDegreeAvaStats.get(table).get(scaledClosestJD).ids;

        initializeIDStartingIndexes(rvIDStartingIndex, scaledClosestRV, jdIDStartingIndex, scaledClosestJD);
        int incrementalFrequency = calculateIncrementalFrequency(scaledRVIDs, rvIDStartingIndex, scaledClosestRV, scaledJDIDs, jdIDStartingIndex, scaledClosestJD, expectedFrequency);

        ArrayList<ArrayList<Integer>> scaledKV = formScaledKV(scaledClosestJD, scaledClosestRV, kvToIDsWithoutTable);

        int rvIndex = rvIDStartingIndex.get(scaledClosestRV);
        int jdIndex = jdIDStartingIndex.get(scaledClosestJD);

        updateIDMaps(incrementalFrequency, mapJDToRV, scaledJDIDs, scaledRVIDs, rvIndex, scaledKV, kvToIDsWithoutTable, jdIndex);
        expectedFrequency -= incrementalFrequency;

        updateIDStartingIndex(scaledClosestJD, incrementalFrequency, jdIndex, scaledClosestRV, rvIndex, rvIDStartingIndex, jdIDStartingIndex);

        cleanRVIDsIfNecessary(scaledRVIDs, incrementalFrequency, rvIndex, scaledRVSumMap, scaledClosestRV);
        cleanJDIDsIfNecessary(scaledJDIDs, incrementalFrequency, jdIndex, scaledJDSumMap, scaledClosestJD);
        return expectedFrequency;
    }

}
