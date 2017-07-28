/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dscaler.dataStruct;

import db.structs.ComKey;
import db.structs.DB;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaLoadCorr implements Runnable {

    Map.Entry<String, ArrayList<ComKey>> entry;
    HashMap<String, ArrayList<ComKey>> jointDegreeTable;
    DB originalDB;
    CoDa originalCoDa;

    @Override
    public void run() {

        ArrayList<ArrayList<ArrayList<Integer>>> tupleRVDegree = new ArrayList<>();

        HashMap<ArrayList<ArrayList<Integer>>, Integer> rvCounts = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvHashMap = new HashMap<>();
        HashMap<Integer, ArrayList<ComKey>> combinedKeys = new HashMap<>();

        for (int i = 0; i < entry.getValue().size(); i++) {
            combinedKeys.put(i, jointDegreeTable.get(entry.getValue().get(i).sourceTable));
        }

        String curTable = entry.getKey();
        int tableID = this.originalDB.getTableID(curTable);
        for (int keyMappedID = 0; keyMappedID < originalDB.getTableSize(curTable); keyMappedID++) {

            int[] tupleValue = originalDB.getFKValues(tableID, keyMappedID);
            ArrayList<ArrayList<Integer>> rvDegree = new ArrayList<>(combinedKeys.size());

            for (int i = 0; i < combinedKeys.size(); i++) {
                rvDegree.add(this.originalCoDa.jointDegrees.get(combinedKeys.get(i)).get(tupleValue[i]));

                if (this.originalCoDa.jointDegrees.get(combinedKeys.get(i)).get(tupleValue[i]) == null) {
                    System.out.println(combinedKeys.get(i) + "-----" + keyMappedID + " "
                            + tupleValue[i] + "      " + i + "      " + entry.getKey());
                }
            }
            
            tupleRVDegree.add(rvDegree);
            if (!rvCounts.containsKey(rvDegree)) {
                rvCounts.put(rvDegree, 1);
            } else {
                rvCounts.put(rvDegree, 1 + rvCounts.get(rvDegree));
            }

            if (!rvHashMap.containsKey(rvDegree)) {
                rvHashMap.put(rvDegree, new ArrayList<Integer>());
            }
            rvHashMap.get(rvDegree).add(keyMappedID);
        }

        this.originalCoDa.reverseRVs.put(curTable, rvHashMap);
        this.originalCoDa.tupleRVs.put(curTable, tupleRVDegree);
        this.originalCoDa.rvDistribution.put(curTable, rvCounts);
    }

}
