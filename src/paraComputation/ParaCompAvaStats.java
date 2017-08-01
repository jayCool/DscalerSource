/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paraComputation;


import db.structs.ComKey;
import dataStructure.AvaliableStatistics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaCompAvaStats implements Runnable {

    Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry;
    HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats;
    //HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts;

    public ParaCompAvaStats(
            Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats
    ) {
        this.jointDegreeEntry = jointDegreeEntry;
        this.jointDegreeAvaStats = jointDegreeAvaStats;
      //  this.ckJDAvaCounts = ckJDAvaCounts;

    }

    @Override
    public void run() {
        int startID = 0;
        HashMap<ArrayList<Integer>, AvaliableStatistics> jdAvaStats = new HashMap<>();
        /** for (int i = 0; i < jointDegreeEntry.getKey().size(); i++) {
            ckJDAvaCounts.put(jointDegreeEntry.getKey().get(i), new HashMap<ArrayList<Integer>, Integer>());
        }*/
       
        for (Map.Entry<ArrayList<Integer>, Integer> jointDegreeFreqEntry : jointDegreeEntry.getValue().entrySet()) {
           
            int[] startid = new int[jointDegreeEntry.getKey().size()];
            int[] ids = new int[jointDegreeFreqEntry.getValue()];
            for (int j = 0; j < jointDegreeFreqEntry.getValue(); j++) {
                ids[j] = j + startID;
            }
            AvaliableStatistics avaStat = new AvaliableStatistics();

            
            int[] ckAvaCounts = new int[jointDegreeFreqEntry.getKey().size()];
            for (int i = 0; i < jointDegreeEntry.getKey().size(); i++) {
                int c = jointDegreeFreqEntry.getValue() * jointDegreeFreqEntry.getKey().get(i);
            //    ckJDAvaCounts.get(jointDegreeEntry.getKey().get(i)).put(jointDegreeFreqEntry.getKey(), c);
                ckAvaCounts[i] = c;
            }
            startID += jointDegreeFreqEntry.getValue();

            avaStat.ids = ids;
            avaStat.startIndex = startid;
            
            avaStat.ckAvaCount = ckAvaCounts;
            
            jdAvaStats.put(jointDegreeFreqEntry.getKey(), avaStat);

        }
        jointDegreeAvaStats.put(jointDegreeEntry.getKey().get(0).sourceTable, jdAvaStats);
    }

}
