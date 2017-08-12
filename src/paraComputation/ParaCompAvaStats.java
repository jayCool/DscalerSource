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
        int sum = 0;
        int index = -1;
        if (jointDegreeEntry.getKey().get(0).sourceTable.equals("user")) {
            for (int j = 0; j < jointDegreeEntry.getKey().size(); j++) {
                ComKey ck = jointDegreeEntry.getKey().get(j);
                if (ck.getReferencingTable().equals("diary_commment")) {
                    index = j;
                }
            }
        }
        if (jointDegreeEntry.getKey().get(0).sourceTable.equals("diary")) {
            for (int j = 0; j < jointDegreeEntry.getKey().size(); j++) {
                ComKey ck = jointDegreeEntry.getKey().get(j);
                if (ck.getReferencingTable().equals("diary_commment")) {
                    index = j;
                }
            }
        }
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
                if (index >= 0) {
                    sum += c;
                }
                ckAvaCounts[i] = c;
            }
            startID += jointDegreeFreqEntry.getValue();

            avaStat.ids = ids;
            avaStat.startIndex = startid;

            avaStat.ckAvaCount = ckAvaCounts;

            jdAvaStats.put(jointDegreeFreqEntry.getKey(), avaStat);

        }
        if (index >= 0) {
            System.err.println(jointDegreeEntry.getKey().get(index).toString() +"\t" +sum);
        }

        jointDegreeAvaStats.put(jointDegreeEntry.getKey().get(0).sourceTable, jdAvaStats);
    }

}
