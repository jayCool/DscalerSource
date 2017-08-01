package paraComputation;

import db.structs.ComKey;
import dataStructure.AvaliableStatistics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Zhang Jiangwei
 */
public class ParaCalculateJDSum implements Runnable {

    HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jointDegreeSumMap;
    Map.Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStatsEntry;
    ComKey ck;
    boolean terminated = false;

    public ParaCalculateJDSum(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jointDegreeSumMap,
            Map.Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStatsEntry, ComKey ck) {
        this.jointDegreeSumMap = jointDegreeSumMap;
        this.jointDegreeAvaStatsEntry = jointDegreeAvaStatsEntry;
        this.ck = ck;
    }

    @Override
    public void run() {
        HashMap<Integer, ArrayList<ArrayList<Integer>>> temp = this.calJointDegreeSum(jointDegreeAvaStatsEntry.getValue().keySet());
        this.jointDegreeSumMap.put(ck, temp);
        terminated = true;
    }

    public void singlerun() {
        HashMap<Integer, ArrayList<ArrayList<Integer>>> sumDistance = this.calJointDegreeSum(jointDegreeAvaStatsEntry.getValue().keySet());
        this.jointDegreeSumMap.put(ck, sumDistance);
        terminated = true;
    }

    /**
     * This method calculates returns a map, 
     * the KEY is the joint-degree sum,
     * the VALUE are the joint-degrees having that sum.
     * 
     * @param jointDegreeAvaStatsKeySet
     * @return jointDegreeSumMap
     */
    private HashMap<Integer, ArrayList<ArrayList<Integer>>> calJointDegreeSum(Set<ArrayList<Integer>> jointDegreeAvaStatsKeySet) {
        HashMap<Integer, ArrayList<ArrayList<Integer>>> jdSumHashMap = new HashMap<>();
        for (ArrayList<Integer> jointDegree : jointDegreeAvaStatsKeySet) {
            int jdSum = 0;
            for (int i : jointDegree) {
                jdSum += i;
            }

            if (!jdSumHashMap.containsKey(jdSum)) {
                jdSumHashMap.put(jdSum, new ArrayList<ArrayList<Integer>>());
            }
            jdSumHashMap.get(jdSum).add(jointDegree);
        }
        return jdSumHashMap;
    }
}
