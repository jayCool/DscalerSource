/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara.paraComputation;


import db.structs.ComKey;
import dscaler.dataStruct.AvaliableStatistics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaCompAvaStats implements Runnable {

    Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry;
    HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats;
    HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts;

    public ParaCompAvaStats(
            Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats, 
            HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts
    ) {
        this.jointDegreeEntry = jointDegreeEntry;
        this.srcJDAvaStats = srcJDAvaStats;
        this.ckJDAvaCounts = ckJDAvaCounts;

    }

    @Override
    public void run() {
        int startID = 0;
        HashMap<ArrayList<Integer>, AvaliableStatistics> jdAvaStats = new HashMap<>();
        for (int i = 0; i < jointDegreeEntry.getKey().size(); i++) {
            ckJDAvaCounts.put(jointDegreeEntry.getKey().get(i), new HashMap<ArrayList<Integer>, Integer>());
        }
        for (Map.Entry<ArrayList<Integer>, Integer> jointDegreeFreqEntry : jointDegreeEntry.getValue().entrySet()) {
           
            int[] startid = new int[jointDegreeEntry.getKey().size()];
            int[] ids = new int[jointDegreeFreqEntry.getValue()];
            for (int j = 0; j < jointDegreeFreqEntry.getValue(); j++) {
                ids[j] = j + startID;
            }
            AvaliableStatistics avaStat = new AvaliableStatistics();

            for (int i = 0; i < jointDegreeEntry.getKey().size(); i++) {
                int c = jointDegreeFreqEntry.getValue() * jointDegreeFreqEntry.getKey().get(i);
                ckJDAvaCounts.get(jointDegreeEntry.getKey().get(i)).put(jointDegreeFreqEntry.getKey(), c);
            }
            startID += jointDegreeFreqEntry.getValue();

            avaStat.ids = ids;
            avaStat.start = startid;
            
            jdAvaStats.put(jointDegreeFreqEntry.getKey(), avaStat);

        }
        srcJDAvaStats.put(jointDegreeEntry.getKey().get(0).sourceTable, jdAvaStats);
    }

}
