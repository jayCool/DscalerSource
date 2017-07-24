/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara.paraComputation;

import db.structs.ComKey;
import db.structs.DB;
import dscaler.dataStruct.AvaliableStatistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */


public class ParaIdAssign implements Runnable {

    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationEntry;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution;
    public HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> avaInfo;
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    public HashMap<String, Integer> scaleTableSize;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseRVDistribution;
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> rvSumMap = new HashMap<>();

    public String outPath;
    public DB originalDB;
    int tableNum = 0;
    int fkSize = 0;
    String referTable = "";
    int[] fkTableIndexes;

    public HashMap<String, ArrayList<ComKey>> referencingTable;
    int id = 1;
    boolean noKey = true;
    long[] scaledSourceTableSize = new long[2];
    ComKey firstCK;
    int firstSourceTableIndex, secondSourceTableIndex;
    ComKey secondCK;
    public String delimiter = "\t";
    HashSet<Long> usedId = new HashSet<>();
    Random rand = new Random();
    int calRVSize = 0;

    public ParaIdAssign(
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationEntry,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> avaInfo) {
        this.scaledCorrelationEntry = scaledCorrelationEntry;
        this.scaledCorrelationDistribution = scaledCorrelationDistribution;
        this.avaInfo = avaInfo;
    }

    void newAssigning() throws IOException {
        File file = new File(outPath + "/" + scaledCorrelationEntry.getKey() + ".txt");
        FileWriter writer = new FileWriter(file);
        BufferedWriter pw = new BufferedWriter(writer, 100000);

        initializeParameters();
        mapRVBasedOnSum(reverseRVDistribution.get(scaledCorrelationEntry.getKey()).keySet());
        boolean flag = false;
        int printed = 0;

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledCorrelationPair : scaledCorrelationEntry.getValue().entrySet()) {

            if (scaledCorrelationPair.getKey().size() == 1) {
                if (!flag) {
                    firstCK = referencingTable.get(scaledCorrelationEntry.getKey()).get(0);
                    firstSourceTableIndex = this.mergedDegreeTitle.get(firstCK.sourceTable).indexOf(firstCK);
                    flag = true;
                }
                oneKeyGen(scaledCorrelationPair, pw);
            } else if (scaledCorrelationPair.getKey().size() == 2 && scaledCorrelationPair.getValue() > 0) {
                if (!flag) {
                    firstCK = referencingTable.get(scaledCorrelationEntry.getKey()).get(0);
                    secondCK = referencingTable.get(scaledCorrelationEntry.getKey()).get(1);
                    firstSourceTableIndex = this.mergedDegreeTitle.get(firstCK.sourceTable).indexOf(firstCK);
                    secondSourceTableIndex = this.mergedDegreeTitle.get(secondCK.sourceTable).indexOf(secondCK);
                    flag = true;
                }
                printed += scaledCorrelationPair.getValue();
                twoKeyGen(scaledCorrelationPair, pw);
            } else {
                multipleKeyGen(scaledCorrelationPair, pw);
            }
        }

        notEnoughPrinting(printed, pw);

        pw.close();

        reverseRVDistribution.remove(this.referTable);
        scaledCorrelationDistribution.remove(this.referTable);

    }

    @Override
    public void run() {
        try {
            newAssigning();
        } catch (IOException ex) {
            Logger.getLogger(ParaIdAssign.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void mapRVBasedOnSum(Set<ArrayList<ArrayList<Integer>>> rvSets) {

        for (ArrayList<ArrayList<Integer>> rv : rvSets) {
            int sum = 0;
            for (ArrayList<Integer> jd : rv) {
                for (int degree : jd) {
                    sum += degree;
                }
            }
            if (!rvSumMap.containsKey(sum)) {
                rvSumMap.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            rvSumMap.get(sum).add(rv);
        }

    }

    private void oneKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> scaledCorrelationPair, BufferedWriter pw) throws IOException {
        int k = 0;
        ArrayList<Integer> jointDegree = scaledCorrelationPair.getKey().get(k);
        int frequency = scaledCorrelationPair.getValue();

        int[] queues = this.avaInfo.get(firstCK.sourceTable).get(jointDegree).ids;
        ArrayList<ArrayList<Integer>> calRV = calNearestDegree(scaledCorrelationPair.getKey());

        averagePrinting(frequency, queues, calRV, pw);
        remainderPrinting(frequency, queues, jointDegree, calRV, pw);

    }

    private void twoKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> scaledCorrelationPair, BufferedWriter pw) throws IOException {
        int frequency = scaledCorrelationPair.getValue();
        System.err.println("avaInfo: " + avaInfo);
        System.err.println("firstCK.sourceTable: " + firstCK.sourceTable);
        System.err.println("scaledCorrelationPair.getKey()" + scaledCorrelationPair.getKey());
        
        int queue1Length = this.avaInfo.get(firstCK.sourceTable).get(scaledCorrelationPair.getKey().get(0)).ids.length;
        int fk1StartingIndex = this.avaInfo.get(firstCK.sourceTable).get(scaledCorrelationPair.getKey().get(0)).start[firstSourceTableIndex];

        int[] firstFKIDFrequency = new int[queue1Length];

        fk1StartingIndex = computeFirstFKIDFrequency(frequency, queue1Length, firstFKIDFrequency, fk1StartingIndex);
        this.avaInfo.get(firstCK.sourceTable).get(scaledCorrelationPair.getKey().get(0)).start[firstSourceTableIndex] = fk1StartingIndex;

        int queue2Length = this.avaInfo.get(secondCK.sourceTable).get(scaledCorrelationPair.getKey().get(1)).ids.length;
        int fk2StartingIndex = this.avaInfo.get(secondCK.sourceTable).get(scaledCorrelationPair.getKey().get(1)).start[secondSourceTableIndex];
        
        ArrayList<ArrayList<Integer>> calRV = calNearestDegree(scaledCorrelationPair.getKey());

        for (int fk1ID = 0; fk1ID < firstFKIDFrequency.length; fk1ID++) {
            for (int idFrequency = 0; idFrequency < firstFKIDFrequency[fk1ID]; idFrequency++) {
                printFile(avaInfo.get(firstCK.sourceTable).get(scaledCorrelationPair.getKey().get(0)).ids[fk1ID],
                        avaInfo.get(secondCK.sourceTable).get(scaledCorrelationPair.getKey().get(1)).ids[fk2StartingIndex], id, idAssign(calRV), pw);

                fk2StartingIndex++;
                fk2StartingIndex = fk2StartingIndex % queue2Length;
                id++;
            }
        }
        //calRV = null;
        firstFKIDFrequency = null;
        this.avaInfo.get(secondCK.sourceTable).get(scaledCorrelationPair.getKey().get(1)).start[secondSourceTableIndex] = fk2StartingIndex;
    }

    private void multipleKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> scaledCorrelationPair, BufferedWriter pw
    ) throws IOException {
        String[][] fkIDs = new String[scaledCorrelationPair.getKey().size()][scaledCorrelationPair.getValue()];
        int frequency = scaledCorrelationPair.getValue();

        for (int k = 0; k < scaledCorrelationPair.getKey().size() && scaledCorrelationPair.getValue() != 0; k++) {
            ArrayList<Integer> jointDegree = scaledCorrelationPair.getKey().get(k);
            int[] idsWithJD = this.avaInfo.get(referencingTable.get(scaledCorrelationEntry.getKey()).get(k).sourceTable).get(jointDegree).ids;
            int startingIDindex = this.avaInfo.get(referencingTable.get(scaledCorrelationEntry.getKey()).get(k).sourceTable).get(jointDegree).start[fkTableIndexes[k]];
            for (int i = 0; i < frequency; i++) {
                int jdID = idsWithJD[startingIDindex];
                startingIDindex++;
                startingIDindex = startingIDindex % idsWithJD.length;
                fkIDs[k][i] = "" + jdID;
            }
            this.avaInfo.get(referencingTable.get(scaledCorrelationEntry.getKey()).get(k).sourceTable).get(jointDegree).start[fkTableIndexes[k]] = startingIDindex;
        }

        String[] combinedKeys = fkIDs[0];
        for (int k = 1; k < fkIDs.length; k++) {
            combinedKeys = combineKeys(combinedKeys, fkIDs[k]);
        }

        ArrayList<ArrayList<Integer>> nearDegree = calNearestDegree(scaledCorrelationPair.getKey());
        for (int i = 0; i < combinedKeys.length; i++) {
            int matchID = idAssign(nearDegree);
            printFile(combinedKeys[i], id, matchID, pw);
            id++;
        }

        //nearDegree = null;
    }

    private int idAssign(ArrayList<ArrayList<Integer>> calDeg) {

        int candidateIndex = rand.nextInt(calRVSize);

        return reverseRVDistribution.get(scaledCorrelationEntry.getKey()).get(calDeg).get(candidateIndex);

    }

    private ArrayList<ArrayList<Integer>> nearestSum(ArrayList<ArrayList<Integer>> rv) {
        int sum1 = getSum(rv);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (rvSumMap.containsKey(sum1 + i)) {
                int size = rvSumMap.get(sum1 + i).size();
                if (size == 0) {
                    rvSumMap.remove(sum1 + i);
                } else {
                    int candidateIndex = (int) (Math.random() * (size - 1) + 0.45);
                    return rvSumMap.get(sum1 + i).get(candidateIndex);
                }
            }
            if (rvSumMap.containsKey(sum1 - i)) {
                int size = rvSumMap.get(sum1 - i).size();
                int candidateIndex = (int) (Math.random() * (size - 1) + 0.45);
                return rvSumMap.get(sum1 - i).get(candidateIndex);
            }
        }
        return null;
    }

    private int getSum(ArrayList<ArrayList<Integer>> rv) {
        int sum = 0;
        for (ArrayList<Integer> jointdegree : rv) {
            for (int degree : jointdegree) {
                sum += degree;
            }
        }
        return sum;
    }

    private ArrayList<ArrayList<Integer>> calNearestDegree(ArrayList<ArrayList<Integer>> rv) {
        if (reverseRVDistribution.get(scaledCorrelationEntry.getKey()).containsKey(rv)) {
            ArrayList<ArrayList<Integer>> calRV = rv;
            calRVSize = reverseRVDistribution.get(scaledCorrelationEntry.getKey()).get(calRV).size();
            return calRV;
        }
        ArrayList<ArrayList<Integer>> calRV = nearestSum(rv);
        while (reverseRVDistribution.get(scaledCorrelationEntry.getKey()).get(calRV).isEmpty()) {
            reverseRVDistribution.get(scaledCorrelationEntry.getKey()).remove(calRV);
            int rvDegreeSum = getSum(calRV);
            rvSumMap.get(rvDegreeSum).remove(calRV);
            if (rvSumMap.get(rvDegreeSum).isEmpty()) {
                rvSumMap.remove(rvDegreeSum);
            }
            calRV = nearestSum(rv);
        }

        calRVSize = reverseRVDistribution.get(scaledCorrelationEntry.getKey()).get(calRV).size();
        return calRV;
    }

    private void printFile(int[] fkIDs, int pkID, int matchedOriginalPKID, BufferedWriter pw) throws IOException {
        pw.write("" + pkID);
        for (int fkID : fkIDs) {
            pw.write(delimiter + fkID);
        }
        if (this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID] != null) {
            pw.write(delimiter + this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID]);
        }

        pw.newLine();
    }

    private void printFile(int fk1, int pkID, int matchedOriginalPKID, BufferedWriter pw) throws IOException {
        pw.write("" + pkID);
        pw.write(delimiter + fk1);
        if (this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID] != null) {
            pw.write(delimiter + this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID]);
        }

        pw.newLine();
    }

    private void printFile(long fk1, long fk2, int pkID, int matchedOriginalPKID, BufferedWriter pw) throws IOException {
        pw.write("" + pkID);
        pw.write(delimiter + fk1 + delimiter + fk2);
        usedId.add(fk1 * scaledSourceTableSize[0] + fk2);
        if (this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID] != null) {
            pw.write(delimiter + this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID]);
        }

        pw.newLine();
    }

    private void initializeParameters() {
        tableNum = this.originalDB.getTableID(scaledCorrelationEntry.getKey());
        fkSize = this.originalDB.tables[tableNum].fkSize;
        referTable = scaledCorrelationEntry.getKey();
        fkTableIndexes = new int[referencingTable.get(referTable).size()];

        for (int i = 0; i < fkTableIndexes.length; i++) {
            ComKey ck = referencingTable.get(referTable).get(i);
            fkTableIndexes[i] = this.mergedDegreeTitle.get(ck.sourceTable).indexOf(ck);
        }

        if (referencingTable.get(referTable).size() == 2) {
            for (int i = 0; i < referencingTable.get(referTable).size(); i++) {
                String s = referencingTable.get(referTable).get(i).sourceTable;
                scaledSourceTableSize[i] = this.scaleTableSize.get(s);
            }
        }

        id = 0;
    }

    private void averagePrinting(int frequency, int[] queues, ArrayList<ArrayList<Integer>> calRV, BufferedWriter pw) throws IOException {
        for (int i = 0; i < frequency / queues.length * queues.length; i++) {
            printFile(queues[i % queues.length], id, idAssign(calRV), pw);
            id++;
        }
    }

    private void remainderPrinting(int frequency, int[] queues, ArrayList<Integer> jointDegree, ArrayList<ArrayList<Integer>> calRV, BufferedWriter pw) throws IOException {
        int fk1StartingIndex = this.avaInfo.get(firstCK.sourceTable).get(jointDegree).start[firstSourceTableIndex];
        for (int i = frequency / queues.length * queues.length; i < frequency; i++) {
            printFile(queues[fk1StartingIndex], id, idAssign(calRV), pw);
            fk1StartingIndex++;
            fk1StartingIndex = fk1StartingIndex % queues.length;
            id++;
        }
        //calRV = null;
        this.avaInfo.get(firstCK.sourceTable).get(jointDegree).start[firstSourceTableIndex] = fk1StartingIndex;
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

    private String[] combineKeys(String[] combinedKeys, String[] fkID) {
        Arrays.sort(combinedKeys);
        for (int i = 0; i < combinedKeys.length; i++) {
            combinedKeys[i] += delimiter + fkID[i];
        }
        return combinedKeys;
    }

    private void printFile(String fks, int pkID, int matchedOriginalPKID, BufferedWriter pw) throws IOException {
        pw.write("" + pkID + delimiter + fks);

        if (this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID] != null) {
            pw.write(delimiter + this.originalDB.tables[this.tableNum].nonKeys[matchedOriginalPKID]);
        }

        pw.newLine();
    }

    private void notEnoughPrinting(int printed, BufferedWriter pw) throws IOException {
        if (referencingTable.get(referTable).size() == 2 && printed < this.scaleTableSize.get(this.referTable)) {
            ArrayList<Integer> fk1IDset = new ArrayList<>();
            for (int i = 0; i < scaledSourceTableSize[0]; i++) {
                fk1IDset.add(i);
            }

            while (printed < this.scaleTableSize.get(this.referTable)) {
                System.out.println("In Sufficient Tuple--Adding " + (printed - this.scaleTableSize.get(this.referTable)));
                Collections.shuffle(fk1IDset);
                for (long i = 0; i < fk1IDset.size() && printed < this.scaleTableSize.get(this.referTable); i++) {
                    long j = (long) (scaledSourceTableSize[1] * Math.random());
                    long newid = scaledSourceTableSize[0] * i + j;
                    if (!this.usedId.contains(newid)) {
                        pw.write("" + id);
                        pw.write(delimiter + i + delimiter + j);
                        int matchId = (int) Math.floor(this.originalDB.tables[tableNum].nonKeys.length * Math.random());
                        if (this.originalDB.tables[this.tableNum].nonKeys[matchId] != null) {
                            pw.write(delimiter + this.originalDB.tables[this.tableNum].nonKeys[matchId]);
                        }
                        pw.newLine();
                        id++;

                        usedId.add(newid);
                        printed++;
                    }
                }
            }
        }
    }

}
