package dsapara.paraComputation;

import dscaler.dataStruct.AvaliableStatistics;
import dbstrcture.ComKey;
import dscaler.dataStruct.AvaliableStatistics;
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
 * @author workshop
 */
public class ParaCompJDSum implements Runnable {
HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap;
Map.Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStatsEntry;
ComKey ck;
boolean terminated=false;
    public ParaCompJDSum(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap, 
            Map.Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStatsEntry,ComKey ck) {
        this.distanceMap = distanceMap;
        this.srcJDAvaStatsEntry =srcJDAvaStatsEntry;
        this.ck = ck;
    }
    @Override
    public void run() {
         //   System.out.println(ck);
            HashMap<Integer, ArrayList<ArrayList<Integer>>> temp = this.sumKeyDegrees(srcJDAvaStatsEntry.getValue().keySet());
           this.distanceMap.put(ck,temp ); 
           terminated=true;
    }
    
    public void singlerun() {
          //  System.out.println(ck);
            HashMap<Integer, ArrayList<ArrayList<Integer>>> sumDistance = this.sumKeyDegrees(srcJDAvaStatsEntry.getValue().keySet());
           this.distanceMap.put(ck,sumDistance ); 
           terminated=true;
    }
    
      private HashMap<Integer, ArrayList<ArrayList<Integer>>> sumKeyDegrees(Set<ArrayList<Integer>> srcJDAvaStatsKeySet) {
        HashMap<Integer, ArrayList<ArrayList<Integer>>> distance = new HashMap<>();
        for (ArrayList<Integer> jointDegree : srcJDAvaStatsKeySet) {
            int jdSum = 0;
            for (int i : jointDegree) {
                jdSum += i;
            }
            if (!distance.containsKey(jdSum)) {
                distance.put(jdSum, new ArrayList<ArrayList<Integer>>());
            }
            distance.get(jdSum).add(jointDegree);
        }
        return distance;
    }
}
