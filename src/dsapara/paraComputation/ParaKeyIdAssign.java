/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara.paraComputation;

import db.structs.ComKey;
import dscaler.dataStruct.AvaliableStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

/**
 *
 * @author workshop
 */
public class ParaKeyIdAssign implements Runnable {

    public HashMap<String, ArrayList<ArrayList<Integer>>> rvFKids;
    public HashMap<String, int[][]> efficientAssignIDs;
    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationEntry;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution;
    public HashMap<String, ArrayList<ComKey>> referencingTables;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs;
    public HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> avaInfo;
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    public HashMap<String, Integer> scaleTableSize;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseDistribution;

    String referTable = "";
    int id = 0;
    int[] fkTableIndexes;
    ComKey firstCK;
    int firstSourceTableIndex, secondSourceTableIndex;
    ComKey secondCK;
    public String delimiter = "\t";

    public ParaKeyIdAssign(HashMap<String, ArrayList<ArrayList<Integer>>> rvFKids,
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationEntry,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, ArrayList<ComKey>> avaMaps,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> avaInfo) {
        this.rvFKids = rvFKids;
        this.scaledCorrelationEntry = scaledCorrelationEntry;
        this.scaledCorrelationDistribution = scaledCorrelationDistribution;
        this.referencingTables = avaMaps;
        this.referencingIDs = referencingIDs;
        this.avaInfo = avaInfo;
    }

    void newAssigning() {
        initializeParameter();

        boolean flag = false;
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKids = new HashMap<>();

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> correlationEntry : scaledCorrelationEntry.getValue().entrySet()) {

            if (!rvPKids.containsKey(correlationEntry.getKey())) {
                rvPKids.put(correlationEntry.getKey(), new ArrayList<Integer>());
            }

            if (correlationEntry.getKey().size() == 1) {
                if (!flag) {
                    firstCK = referencingTables.get(scaledCorrelationEntry.getKey()).get(0);
                    firstSourceTableIndex = this.mergedDegreeTitle.get(firstCK.sourceTable).indexOf(firstCK);
                    flag = true;
                }
                oneKeyGen(correlationEntry, rvPKids);

            } else if (correlationEntry.getKey().size() == 2 && correlationEntry.getValue() > 0) {
                if (!flag) {
                    firstCK = referencingTables.get(scaledCorrelationEntry.getKey()).get(0);
                    secondCK = referencingTables.get(scaledCorrelationEntry.getKey()).get(1);
                    firstSourceTableIndex = this.mergedDegreeTitle.get(firstCK.sourceTable).indexOf(firstCK);
                    secondSourceTableIndex = this.mergedDegreeTitle.get(secondCK.sourceTable).indexOf(secondCK);
                    flag = true;
                }
                twoKeyGen(correlationEntry, rvPKids);
            } else {
                threeKeyGen(correlationEntry, rvPKids);
            }
        }
        referencingIDs.put(scaledCorrelationEntry.getKey(), rvPKids);
    }

    @Override
    public void run() {
        newAssigning();
    }

    private void oneKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> correlationEntry,
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKids
    ) {
        int k = 0;
        ArrayList<Integer> jointDegree = correlationEntry.getKey().get(k);
        int frequency = correlationEntry.getValue();
        int[] queues = this.avaInfo.get(firstCK.sourceTable).get(jointDegree).ids;

        averageAllocation(frequency, queues, rvPKids, correlationEntry);
        remainderAllocation(frequency, queues, rvPKids, correlationEntry, jointDegree);

    }

    private void twoKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> correlationEntry,
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKids
    ) {
        int frequency = correlationEntry.getValue();
        int queue1Length = this.avaInfo.get(firstCK.sourceTable).get(correlationEntry.getKey().get(0)).ids.length;
        int fk1StartingIndex = this.avaInfo.get(firstCK.sourceTable).get(correlationEntry.getKey().get(0)).startIndex[firstSourceTableIndex];

        int[] firstFKIDFrequency = new int[queue1Length];
        fk1StartingIndex = computeFirstFKIDFrequency(frequency, queue1Length, firstFKIDFrequency, fk1StartingIndex);
        this.avaInfo.get(firstCK.sourceTable).get(correlationEntry.getKey().get(0)).startIndex[firstSourceTableIndex] = fk1StartingIndex;

        int queue2Length = this.avaInfo.get(secondCK.sourceTable).get(correlationEntry.getKey().get(1)).ids.length;
        int fk2StartingIndex = this.avaInfo.get(secondCK.sourceTable).get(correlationEntry.getKey().get(1)).startIndex[secondSourceTableIndex];

        for (int fk1ID = 0; fk1ID < firstFKIDFrequency.length; fk1ID++) {
            for (int idFrequency = 0; idFrequency < firstFKIDFrequency[fk1ID]; idFrequency++) {
                ArrayList<Integer> fkIDs = new ArrayList<>();
                fkIDs.add(avaInfo.get(firstCK.sourceTable).get(correlationEntry.getKey().get(0)).ids[fk1ID]);
                fkIDs.add(avaInfo.get(secondCK.sourceTable).get(correlationEntry.getKey().get(1)).ids[fk2StartingIndex]);
                this.rvFKids.get(this.referTable).add(fkIDs);
                fk2StartingIndex++;
                fk2StartingIndex = fk2StartingIndex % queue2Length;
                rvPKids.get(correlationEntry.getKey()).add(id);
                id++;
            }
        }
        //firstFKIDFrequency = null;
        this.avaInfo.get(secondCK.sourceTable).get(correlationEntry.getKey().get(1)).startIndex[secondSourceTableIndex] = fk2StartingIndex;
    }

    private void threeKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> scaledCorrelationPair, 
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKids
    ) {
         String[][] fkIDs = new String[scaledCorrelationPair.getKey().size()][scaledCorrelationPair.getValue()];
        int frequency = scaledCorrelationPair.getValue();

        for (int k = 0; k < scaledCorrelationPair.getKey().size() && scaledCorrelationPair.getValue() != 0; k++) {
            ArrayList<Integer> jointDegree = scaledCorrelationPair.getKey().get(k);
            int[] idsWithJD = this.avaInfo.get(referencingTables.get(scaledCorrelationEntry.getKey()).get(k).sourceTable).get(jointDegree).ids;
            int startingIDindex = this.avaInfo.get(referencingTables.get(scaledCorrelationEntry.getKey()).get(k).sourceTable).get(jointDegree).startIndex[fkTableIndexes[k]];
            for (int i = 0; i < frequency; i++) {
                int jdID = idsWithJD[startingIDindex];
                startingIDindex++;
                startingIDindex = startingIDindex % idsWithJD.length;
                fkIDs[k][i] = "" + jdID;
            }
            this.avaInfo.get(referencingTables.get(scaledCorrelationEntry.getKey()).get(k).sourceTable).get(jointDegree).startIndex[fkTableIndexes[k]] = startingIDindex;
        }

        String[] combinedKeys = fkIDs[0];
        for (int k = 1; k < fkIDs.length; k++) {
            combinedKeys = combineKeys(combinedKeys, fkIDs[k]);
        }
        
       
        for (int i = 0; i < combinedKeys.length; i++) {
            rvPKids.get(scaledCorrelationPair.getKey()).add(id);
            ArrayList<Integer> ids = new ArrayList<>();
            for (String fkid: combinedKeys[i].trim().split(delimiter)){
                ids.add(Integer.parseInt(fkid));
            }
            this.rvFKids.get(this.referTable).add(ids);
            id++;
        }
    }

    private void initializeParameter() {
        referTable = scaledCorrelationEntry.getKey();
        fkTableIndexes = new int[referencingTables.get(referTable).size()];
        for (int i = 0; i < fkTableIndexes.length; i++) {
            ComKey ck = referencingTables.get(referTable).get(i);
            fkTableIndexes[i] = this.mergedDegreeTitle.get(ck.sourceTable).indexOf(ck);
        }
        this.rvFKids.put(referTable, new ArrayList<ArrayList<Integer>>());
        id = 0;

    }
    
    
    private String[] combineKeys(String[] combinedKeys, String[] fkID) {
        Arrays.sort(combinedKeys);
        for (int i = 0; i < combinedKeys.length; i++) {
            combinedKeys[i] += "\t" + fkID[i];
        }
        return combinedKeys;
    }
    private int computeFirstFKIDFrequency(int frequency, int queue1Length, int[] firstFKIDFrequency, int fk1StartingIndex) {
        if (frequency >= queue1Length) {
            for (int i = 0; i < queue1Length; i++) {
                firstFKIDFrequency[i] = frequency / queue1Length;
            }
        }

        for (int i = 0; i < frequency - frequency / queue1Length * queue1Length; i++) {
            firstFKIDFrequency[fk1StartingIndex] += 1;
            fk1StartingIndex++;
            fk1StartingIndex = fk1StartingIndex % queue1Length;
        }
        return fk1StartingIndex;
    }

    private void averageAllocation(int frequency, int[] queues, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKids, Entry<ArrayList<ArrayList<Integer>>, Integer> correlationEntry) {
        for (int i = 0; i < frequency / queues.length * queues.length; i++) {
            ArrayList<Integer> fkIDs = new ArrayList<>();
            fkIDs.add((Integer) queues[i % queues.length]);
            this.rvFKids.get(this.referTable).add(fkIDs);
            rvPKids.get(correlationEntry.getKey()).add(id);
            id++;
        }
    }

    private void remainderAllocation(int frequency, int[] queues, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKids, Entry<ArrayList<ArrayList<Integer>>, Integer> correlationEntry, ArrayList<Integer> jointDegree) {
        int fk1StartingIndex = this.avaInfo.get(firstCK.sourceTable).get(jointDegree).startIndex[firstSourceTableIndex];
        for (int i = frequency / queues.length * queues.length; i < frequency; i++) {
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(queues[fk1StartingIndex]);
            this.rvFKids.get(this.referTable).add(tmp);
            fk1StartingIndex++;
            fk1StartingIndex = fk1StartingIndex % queues.length;
            rvPKids.get(correlationEntry.getKey()).add(id);
            id++;
        }
        this.avaInfo.get(firstCK.sourceTable).get(jointDegree).startIndex[firstSourceTableIndex] = fk1StartingIndex;
    }

}
