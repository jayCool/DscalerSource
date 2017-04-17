/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara.paraComputation;

import dscaler.dataStruct.AvaliableStatistics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author workshop
 */
public class ParaMatchKV implements Runnable {

    public HashMap<String, HashMap<Integer, Integer>> scaledBiMap;
    public HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats;
    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVentry;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDis;
    int rvKeySum = 0;
    int jdKeySum = 0;
    public double sRatio;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs;

    private ArrayList<ArrayList<Integer>> calRVFromClosestKeySum(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap,
            ArrayList<ArrayList<Integer>> originalRV) {
        int originalRVSum = calRVSum(originalRV);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (scaledRVSumMap.containsKey(i + originalRVSum) && !scaledRVSumMap.get(i + originalRVSum).isEmpty()) {
                rvKeySum = i + originalRVSum;
                return scaledRVSumMap.get(rvKeySum).get(0);
            }
            if (scaledRVSumMap.containsKey(-i + originalRVSum) && !scaledRVSumMap.get(-i + originalRVSum).isEmpty()) {
                rvKeySum = -i + originalRVSum;
                return scaledRVSumMap.get(rvKeySum).get(0);
            }

        }
        System.out.println("error in finding RV");
        return null;
    }

    private HashMap<Integer, ArrayList<ArrayList<Integer>>> calJDSumMap(Set<ArrayList<Integer>> jointDegreeSet) {

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

    private ArrayList<Integer> calJDFromClosestKeySum(HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap, 
            ArrayList<Integer> originalJD) {
        
        int originalKeySum = calJDSum(originalJD);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (scaledJDSumMap.containsKey(i + originalKeySum) && !scaledJDSumMap.get(i + originalKeySum).isEmpty()) {
                jdKeySum = i + originalKeySum;
                return scaledJDSumMap.get(jdKeySum).get(0);
            }
            if (scaledJDSumMap.containsKey(-i + originalKeySum) && !scaledJDSumMap.get(-i + originalKeySum).isEmpty()) {
                jdKeySum = -i + originalKeySum;
                return scaledJDSumMap.get(jdKeySum).get(0);
            }
        }
        System.out.println("error in finding closest JD sum");
        return null;
    }

    private int calRVSum(ArrayList<ArrayList<Integer>> rv) {
        int rvSum = 0;
        for (ArrayList<Integer> jointDegree : rv) {
            for (int degree : jointDegree) {
                rvSum += degree;
            }
        }
        return rvSum;
    }

    private HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> calRVSumMap(Set<ArrayList<ArrayList<Integer>>> rvSet) {
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> rvSumMap = new HashMap<>();
        for (ArrayList<ArrayList<Integer>> rv : rvSet) {
            int rvSum = calRVSum(rv);

            if (!rvSumMap.containsKey(Math.abs(rvSum))) {
                rvSumMap.put(Math.abs(rvSum), new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            rvSumMap.get(Math.abs(rvSum)).add(rv);
        }
        return rvSumMap;
    }

    @Override
    public void run() {
        newMatch();
    }

    private void newMatch() {
        String table = scaledRVentry.getKey();
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> keyIDs = new HashMap<>();
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap = calRVSumMap(scaledRVentry.getValue().keySet());
        HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap = calJDSumMap(srcJDAvaStats.get(table).keySet());

        int sum = 0;
        HashMap<Integer, Integer> mapJD2RV = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> rvCandidateIndex = new HashMap<>();
        HashMap<ArrayList<Integer>, Integer> jdCandidateIndex = new HashMap<>();

        while (scaledRVSumMap.size() > 0 && scaledJDSumMap.size() > 0) {
            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry : originalKVDis.get(table).entrySet()) {
                if (scaledRVSumMap.isEmpty() || scaledJDSumMap.isEmpty()) {
                    break;
                }
                int frequency = calFrequency(originalKVentry.getValue());
                ArrayList<ArrayList<Integer>> originalRV = extractOriginalRV(originalKVentry);
                ArrayList<Integer> originalJD = originalKVentry.getKey().get(0);

                while (frequency > 0 && !scaledRVSumMap.isEmpty() && !scaledJDSumMap.isEmpty()) {
                    ArrayList<ArrayList<Integer>> scaleRV = calClosestRV(scaledRVSumMap, originalRV);
                    ArrayList<Integer> scaleJD = calClosestJD(table, originalJD, scaledJDSumMap);
                    ArrayList<Integer> scaledRVIDs = scaledRVentry.getValue().get(scaleRV);
                    int[] scaledJDIDs = srcJDAvaStats.get(table).get(scaleJD).ids;

                    if (!rvCandidateIndex.containsKey(scaleRV)) {
                        rvCandidateIndex.put(scaleRV, 0);
                    }

                    if (!jdCandidateIndex.containsKey(scaleJD)) {
                        jdCandidateIndex.put(scaleJD, 0);
                    }
                    
                    int minFreq = Math.min(scaledRVIDs.size() - rvCandidateIndex.get(scaleRV), 
                            scaledJDIDs.length - jdCandidateIndex.get(scaleJD));

                    minFreq = Math.min(minFreq, frequency);
                    ArrayList<ArrayList<Integer>> scalekeyVec = new ArrayList<>();
                    scalekeyVec.add(scaleJD);
                    scalekeyVec.addAll(scaleRV);

                    int rvIndex = rvCandidateIndex.get(scaleRV);
                    int jdIndex = jdCandidateIndex.get(scaleJD);
                    if (!keyIDs.containsKey(scalekeyVec)) {
                        keyIDs.put(scalekeyVec, new ArrayList<Integer>());
                    }
                    for (int i = 0; i < minFreq; i++) {
                        frequency--;
                        int rvID = scaledRVIDs.get(i + rvIndex);
                        int jdID = scaledJDIDs[i + jdIndex];
                        mapJD2RV.put(jdID, rvID);
                        keyIDs.get(scalekeyVec).add(jdID);
                        sum++;
                    }
                    
                    rvCandidateIndex.put(scaleRV, minFreq + rvIndex);
                    jdCandidateIndex.put(scaleJD, minFreq + jdIndex);

                    if (scaledRVIDs.size() == (minFreq + rvIndex)) {
                        scaledRVSumMap.get(rvKeySum).remove(scaleRV);
                        if (scaledRVSumMap.get(rvKeySum).size() == 0) {
                            scaledRVSumMap.remove(rvKeySum);
                        }
                    }

                    if (scaledJDIDs.length == (minFreq + jdIndex)) {
                        scaledJDSumMap.get(jdKeySum).remove(scaleJD);
                        if (scaledJDSumMap.get(jdKeySum).size() == 0) {
                            scaledJDSumMap.remove(jdKeySum);
                        }
                    }
                }

            }
        }
    //   System.out.println("sum:" + sum);
        this.keyVectorIDs.put(table, keyIDs);
        scaledBiMap.put(table, mapJD2RV);
    }

    private int calJDSum(ArrayList<Integer> jointDegree) {
        int jdSum = 0;
        for (Integer degree : jointDegree) {
            jdSum += degree;
        }
        return jdSum;
    }

    private ArrayList<Integer> calClosestJD(String table, ArrayList<Integer> originalJD,
            HashMap<Integer, ArrayList<ArrayList<Integer>>> scaledJDSumMap) {

        ArrayList<Integer> scaleJD = new ArrayList<>();
        if (srcJDAvaStats.get(table).keySet().contains(originalJD)) {
            scaleJD = originalJD;
            jdKeySum = calJDSum(scaleJD);
           
            if (!scaledJDSumMap.containsKey(jdKeySum) || !scaledJDSumMap.get(jdKeySum).contains(scaleJD)) {
                scaleJD = calJDFromClosestKeySum(scaledJDSumMap, originalJD);
            }
        } else {
         scaleJD = calJDFromClosestKeySum(scaledJDSumMap, originalJD);
        }
        return scaleJD;
    }

    private ArrayList<ArrayList<Integer>> calClosestRV(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> scaledRVSumMap,
            ArrayList<ArrayList<Integer>> originalRV) {
        ArrayList<ArrayList<Integer>> scaledRV = new ArrayList<>();
        if (scaledRVentry.getValue().keySet().contains(originalRV)) {
            scaledRV = originalRV;
            rvKeySum = calRVSum(scaledRV);
            if (!scaledRVSumMap.containsKey(rvKeySum) || !scaledRVSumMap.get(rvKeySum).contains(scaledRV)) {
                scaledRV = calRVFromClosestKeySum(scaledRVSumMap, originalRV);
            }
        } else {
            scaledRV = calRVFromClosestKeySum(scaledRVSumMap, originalRV);
        }
        return scaledRV;

    }

    private int calFrequency(Integer originalFrequency) {
        int scaledFrequency = (int) (originalFrequency * this.sRatio);
        double pro = Math.random();
        if (pro < (originalFrequency * this.sRatio - (int) (originalFrequency * this.sRatio))) {
            scaledFrequency += 1;
        }
        return scaledFrequency;
    }

    private ArrayList<ArrayList<Integer>> extractOriginalRV(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalKVentry) {
        ArrayList<ArrayList<Integer>> originalRV = new ArrayList<>();
        for (int i = 1; i < originalKVentry.getKey().size(); i++) {
            originalRV.add(originalKVentry.getKey().get(i));
        }
        return originalRV;
    }

}
