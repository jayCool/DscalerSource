/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dscaler.dataStruct;

import dscaler.dataStruct.CoDa;
import dbstrcture.ComKey;
import dbstrcture.DB;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author workshop
 */
public class ParaLoadCorr implements Runnable {
    //  HashMap<String, HashSet<String>> missingIDs;

    Map.Entry<String, ArrayList<ComKey>> entry;
    HashMap<String, ArrayList<ComKey>> jointDegreeTable;
    DB originalDB;
    CoDa originalCoDa;

    @Override
    public void run() {

        ArrayList<ArrayList<ArrayList<Integer>>> refDegree = new ArrayList<>();

        HashMap<ArrayList<ArrayList<Integer>>, Integer> degreeCount = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap = new HashMap<>();
        HashMap<Integer, ArrayList<ComKey>> combinedKeys = new HashMap<>();
        for (int i = 0; i < entry.getValue().size(); i++) {
            combinedKeys.put(i, jointDegreeTable.get(entry.getValue().get(i).sourceTable));
        }

        String curTable = entry.getKey();
        int curNum = this.originalDB.tableMapping.get(curTable);
        for (int keyMappedID = 0; keyMappedID < this.originalDB.tables[curNum].fks.length; keyMappedID++) {

            int[] tupleValue = originalDB.tables[curNum].fks[keyMappedID];
            ArrayList<ArrayList<Integer>> degree = new ArrayList<>(combinedKeys.size());
            for (int i = 0; i < combinedKeys.size(); i++) {
                degree.add(this.originalCoDa.jointDegrees.get(combinedKeys.get(i)).get(tupleValue[i]));
                if (this.originalCoDa.jointDegrees.get(combinedKeys.get(i)).get(tupleValue[i]) == null) {
                    System.out.println(combinedKeys.get(i) + "-----" + keyMappedID + " "
                            + tupleValue[i] + "      " + i + "      " + entry.getKey());
                }
            }
                refDegree.add(degree);
                if (!degreeCount.containsKey(degree)) {
                    degreeCount.put(degree, 1);
                } else {
                    degreeCount.put(degree, 1 + degreeCount.get(degree));
                }

                if (!mmap.containsKey(degree)) {
                    mmap.put(degree, new ArrayList<Integer>());
                }
                mmap.get(degree).add(keyMappedID);
        }
        
        
        this.originalCoDa.reverseRVs.put(curTable, mmap);
        this.originalCoDa.idRVs.put(curTable, refDegree);
        this.originalCoDa.rvDis.put(curTable, degreeCount);
    }

}
