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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaReferencingOnlyTableGeneration implements Runnable {

    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVEntry;
    public HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats;
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    public HashMap<String, Integer> scaleTableSize;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseRVDistribution;
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> originalRVSumMap = new HashMap<>();

    public String outPath;
    public DB originalDB;

    public HashMap<String, ArrayList<ComKey>> referencingTable;
    int currentID = 0;

    public String delimiter = "\t";

    Random rand = new Random();
    int calRVSize = 0;

    
    public ParaReferencingOnlyTableGeneration(
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVEntry,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats) {
        this.scaledRVEntry = scaledRVEntry;
        this.jointDegreeAvaStats = jointDegreeAvaStats;
    }

    @Override
    public void run() {
        try {

            BufferedWriter pw = new BufferedWriter(new FileWriter(new File(outPath + "/" + scaledRVEntry.getKey() + ".txt")), 100000);
            String curTable = scaledRVEntry.getKey();
            int tableID = originalDB.getTableID(curTable);
            groupRVsBasedOnSum(originalReverseRVDistribution.get(scaledRVEntry.getKey()).keySet());

            int printed = 0;
            
            switch (originalDB.getReferencingTables(curTable).size()) {

                case 1:
                    printTableForOneKey(tableID, pw);
                    break;
                case 2:
                    printed = printTableForTwoKeys(tableID, pw);
                    break;
                default:
                    printTableForMoreKeys(tableID, pw);
                    break;
            }

            //notEnoughPrinting(tableID, printed, pw, curTable);

            pw.close();
            originalReverseRVDistribution.remove(curTable);
        } catch (IOException ex) {
            Logger.getLogger(ParaReferencingOnlyTableGeneration.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * RVs are grouped based on the sum of the RV
     * @param rvSets 
     */
    private void groupRVsBasedOnSum(Set<ArrayList<ArrayList<Integer>>> rvSets) {

        for (ArrayList<ArrayList<Integer>> rv : rvSets) {
            int sum = 0;
            for (ArrayList<Integer> jd : rv) {
                for (int degree : jd) {
                    sum += degree;
                }
            }
            if (!originalRVSumMap.containsKey(sum)) {
                originalRVSumMap.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            originalRVSumMap.get(sum).add(rv);
        }
    }
    
    /**
     * Output table with only one FK
     * @param tableID
     * @param pw
     * @throws IOException 
     */
    private void printTableForOneKey(int tableID, BufferedWriter pw) throws IOException {

        ComKey firstCK = referencingTable.get(scaledRVEntry.getKey()).get(0);
        int firstCKIndex = this.mergedDegreeTitle.get(firstCK.sourceTable).indexOf(firstCK);

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledRVFrequency : scaledRVEntry.getValue().entrySet()) {
            ArrayList<Integer> jointDegree = scaledRVFrequency.getKey().get(0);
            int frequency = scaledRVFrequency.getValue();

            int[] candidateIDs = jointDegreeAvaStats.get(firstCK.sourceTable).get(jointDegree).ids;
            ArrayList<ArrayList<Integer>> closestRV = calculateClosestRV(scaledRVFrequency.getKey(), scaledRVEntry.getKey());

            printSameAmountOfTuplesForCandidateIDs(tableID, frequency, candidateIDs, closestRV, pw);
            printRemainderOfTuplesForCandidateIDs(firstCK, firstCKIndex, tableID, frequency, candidateIDs, jointDegree, closestRV, pw);
        }
    }
    
    /**
     * Output table with only two FKs
     * @param tableID
     * @param pw
     * @return number of tuples printed
     * @throws IOException 
     */
    private int printTableForTwoKeys(int tableID, BufferedWriter pw) throws IOException {
        ComKey firstCK = referencingTable.get(scaledRVEntry.getKey()).get(0);
        ComKey secondCK = referencingTable.get(scaledRVEntry.getKey()).get(1);
        int firstCKIndex = this.mergedDegreeTitle.get(firstCK.getSourceTable()).indexOf(firstCK);
        int secondCKIndex = this.mergedDegreeTitle.get(secondCK.getSourceTable()).indexOf(secondCK);

        int printedNumber = 0;

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledRVFrequency : scaledRVEntry.getValue().entrySet()) {

            int frequency = scaledRVFrequency.getValue();
            printedNumber += frequency;

            int firstCKIdsLength = this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(scaledRVFrequency.getKey().get(0)).ids.length;
            int firstCKStartingIndex = this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(scaledRVFrequency.getKey().get(0)).startIndex[firstCKIndex];

            int[] firstFKIDFrequencs = new int[firstCKIdsLength];

            firstCKStartingIndex = calculateFirstFKIDFrequencies(frequency, firstCKIdsLength, firstFKIDFrequencs, firstCKStartingIndex);
            this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(scaledRVFrequency.getKey().get(0)).startIndex[firstCKIndex] = firstCKStartingIndex;

            int secondCKIdsLength = this.jointDegreeAvaStats.get(secondCK.getSourceTable()).get(scaledRVFrequency.getKey().get(1)).ids.length;
            int secondCKStartingIndex = this.jointDegreeAvaStats.get(secondCK.getSourceTable()).get(scaledRVFrequency.getKey().get(1)).startIndex[secondCKIndex];

            ArrayList<ArrayList<Integer>> closestRV = calculateClosestRV(scaledRVFrequency.getKey(), scaledRVEntry.getKey());

            for (int firstCKIDIndex = 0; firstCKIDIndex < firstFKIDFrequencs.length; firstCKIDIndex++) {
                for (int idFrequency = 0; idFrequency < firstFKIDFrequencs[firstCKIDIndex]; idFrequency++) {
                    printTuple(tableID, jointDegreeAvaStats.get(firstCK.sourceTable).get(scaledRVFrequency.getKey().get(0)).ids[firstCKIDIndex] + delimiter +
                            jointDegreeAvaStats.get(secondCK.sourceTable).get(scaledRVFrequency.getKey().get(1)).ids[secondCKStartingIndex], currentID, extractOriginalID(closestRV), pw);
                    secondCKStartingIndex++;
                    secondCKStartingIndex = secondCKStartingIndex % secondCKIdsLength;
                    currentID++;
                }
            }
            firstFKIDFrequencs = null;
            this.jointDegreeAvaStats.get(secondCK.sourceTable).get(scaledRVFrequency.getKey().get(1)).startIndex[secondCKIndex] = secondCKStartingIndex;
        }
        return printedNumber;
    }
    
    
    /**
     * Output table with more than two FKs
     * @param tableID
     * @param pw
     * @throws IOException 
     */
    private void printTableForMoreKeys(int tableID,
            BufferedWriter pw
    ) throws IOException {
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledRVFrequency : scaledRVEntry.getValue().entrySet()) {

            String[][] fkIDs = new String[scaledRVFrequency.getKey().size()][scaledRVFrequency.getValue()];
            int frequency = scaledRVFrequency.getValue();
            int[] fkTableIndexes = calculateFKIndexOfJD(tableID);

            for (int k = 0; k < scaledRVFrequency.getKey().size() && scaledRVFrequency.getValue() != 0; k++) {
                ArrayList<Integer> jointDegree = scaledRVFrequency.getKey().get(k);
                int[] idsWithJD = this.jointDegreeAvaStats.get(referencingTable.get(scaledRVEntry.getKey()).get(k).sourceTable).get(jointDegree).ids;
                int startingIDindex = this.jointDegreeAvaStats.get(referencingTable.get(scaledRVEntry.getKey()).get(k).sourceTable).get(jointDegree).startIndex[fkTableIndexes[k]];
                
                for (int i = 0; i < frequency; i++) {
                    int jdID = idsWithJD[startingIDindex];
                    startingIDindex++;
                    startingIDindex = startingIDindex % idsWithJD.length;
                    fkIDs[k][i] = "" + jdID;
                }
                this.jointDegreeAvaStats.get(referencingTable.get(scaledRVEntry.getKey()).get(k).sourceTable).get(jointDegree).startIndex[fkTableIndexes[k]] = startingIDindex;
            }

            String[] combinedFKs = fkIDs[0];
            for (int k = 1; k < fkIDs.length; k++) {
                combinedFKs = calculateCombinedFKs(combinedFKs, fkIDs[k]);
            }

            ArrayList<ArrayList<Integer>> closestRV = calculateClosestRV(scaledRVFrequency.getKey(), scaledRVEntry.getKey());
            for (int i = 0; i < combinedFKs.length; i++) {
                int matchID = extractOriginalID(closestRV);
                printTuple(tableID, combinedFKs[i], currentID, matchID, pw);
                currentID++;
            }
        }

    }

    /**
     *
     * @param closestRV
     * @return ID in original table has closestRV
     */
    private int extractOriginalID(ArrayList<ArrayList<Integer>> closestRV) {
        int candidateIndex = rand.nextInt(calRVSize);
        return originalReverseRVDistribution.get(scaledRVEntry.getKey()).get(closestRV).get(candidateIndex);

    }

    /**
     *
     * @param rv
     * @return closestRV
     */
    private ArrayList<ArrayList<Integer>> calculateClosestRVBasedOnSum(ArrayList<ArrayList<Integer>> rv) {
        int sum = calculateSum(rv);
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (originalRVSumMap.containsKey(sum + i)) {
                result = extractRVsBasedOnSum(sum + i);
            }
            if (result.isEmpty() && originalRVSumMap.containsKey(sum - i)) {
                result = extractRVsBasedOnSum(sum - i);
            }
            if (!result.isEmpty()) {
                return result;
            }
        }
        return null;
    }
    
    
    /**
     * 
     * @param rv
     * @return sum of RV 
     */
    private int calculateSum(ArrayList<ArrayList<Integer>> rv) {
        int sum = 0;
        for (ArrayList<Integer> jointdegree : rv) {
            for (int degree : jointdegree) {
                sum += degree;
            }
        }
        return sum;
    }

    /**
     *
     * @param rv
     * @param curTable
     * @return closestRV
     */
    private ArrayList<ArrayList<Integer>> calculateClosestRV(ArrayList<ArrayList<Integer>> rv, String curTable) {
        if (originalReverseRVDistribution.get(curTable).containsKey(rv)) {
            calRVSize = originalReverseRVDistribution.get(curTable).get(rv).size();
            return rv;
        }

        ArrayList<ArrayList<Integer>> closestRV = calculateClosestRVBasedOnSum(rv);

        while (originalReverseRVDistribution.get(curTable).get(closestRV).isEmpty()) {
            cleanRVForSumMap(curTable, closestRV);
            closestRV = calculateClosestRVBasedOnSum(rv);
        }

        calRVSize = originalReverseRVDistribution.get(curTable).get(closestRV).size();
        return closestRV;
    }
    
    
    /**
     * Print one tuple
     * @param tableID
     * @param fks
     * @param pkID
     * @param matchedOriginalPKID
     * @param pw
     * @throws IOException 
     */
    private void printTuple(int tableID, String fks, int pkID, int matchedOriginalPKID, BufferedWriter pw) throws IOException {
        pw.write("" + pkID + delimiter + fks);

        if (this.originalDB.tables[tableID].nonKeys[matchedOriginalPKID] != null) {
            pw.write(delimiter + this.originalDB.tables[tableID].nonKeys[matchedOriginalPKID]);
        }

        pw.newLine();
    }
    

    /**
     * For IDs with same RV, print the same amount of tuples 
     * @param tableID
     * @param frequency
     * @param candidateIDs
     * @param closestRV
     * @param pw
     * @throws IOException 
     */     
    private void printSameAmountOfTuplesForCandidateIDs(int tableID, int frequency, int[] candidateIDs, ArrayList<ArrayList<Integer>> closestRV, BufferedWriter pw) throws IOException {
        for (int i = 0; i < frequency / candidateIDs.length * candidateIDs.length; i++) {
            printTuple(tableID, ""+candidateIDs[i % candidateIDs.length], currentID, extractOriginalID(closestRV), pw);
            currentID++;
        }
    }
    
    /**
     * Print the left over amount of tuples for the IDs with the same RV
     * @param firstCK
     * @param firstCKIndex
     * @param tableID
     * @param frequency
     * @param candidateIDs
     * @param jointDegree
     * @param closestRV
     * @param pw
     * @throws IOException 
     */
    private void printRemainderOfTuplesForCandidateIDs(ComKey firstCK, int firstCKIndex, int tableID, int frequency,
            int[] candidateIDs, ArrayList<Integer> jointDegree, ArrayList<ArrayList<Integer>> closestRV, BufferedWriter pw) throws IOException {

        int firstCKStartingIndex = this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(jointDegree).startIndex[firstCKIndex];

        for (int i = frequency / candidateIDs.length * candidateIDs.length; i < frequency; i++) {
            printTuple(tableID, ""+candidateIDs[firstCKStartingIndex], currentID, extractOriginalID(closestRV), pw);
            firstCKStartingIndex++;
            firstCKStartingIndex = firstCKStartingIndex % candidateIDs.length;
            currentID++;
        }

        this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(jointDegree).startIndex[firstCKIndex] = firstCKStartingIndex;
    }
    
    /**
     * Calculate the FK appearing frequencies
     * @param frequency
     * @param firstCKIdsLength
     * @param firstFKIDFrequencs
     * @param firstCKStartingIndex
     * @return FKStartingIndex
     */
    private int calculateFirstFKIDFrequencies(int frequency, int firstCKIdsLength, int[] firstFKIDFrequencs, int firstCKStartingIndex) {
        if (frequency >= firstCKIdsLength) {
            for (int i = 0; i < firstCKIdsLength; i++) {
                firstFKIDFrequencs[i] = frequency / firstCKIdsLength;
            }
        }

        for (int i = 0; i < frequency - frequency / firstCKIdsLength * firstCKIdsLength; i++) {
            firstFKIDFrequencs[firstCKStartingIndex] += 1;
            firstCKStartingIndex++;
            firstCKStartingIndex = firstCKStartingIndex % firstCKIdsLength;
        }
        return firstCKStartingIndex;
    }
    
    
    /**
     * Combine the FKs to one string
     * @param combinedKeys
     * @param fkID
     * @return 
     */
    private String[] calculateCombinedFKs(String[] combinedKeys, String[] fkID) {
        Arrays.sort(combinedKeys);
        for (int i = 0; i < combinedKeys.length; i++) {
            combinedKeys[i] += delimiter + fkID[i];
        }
        return combinedKeys;
    }


    private void notEnoughPrinting(int tableID, int printed, BufferedWriter pw, String curTable) throws IOException {
        int[] scaledSourceTableSize = calculateScaledTableSize(curTable);
        HashSet<Long> usedId = new HashSet<>();
        if (referencingTable.get(curTable).size() == 2 && printed < this.scaleTableSize.get(curTable)) {
            ArrayList<Integer> fk1IDset = new ArrayList<>();
            for (int i = 0; i < scaledSourceTableSize[0]; i++) {
                fk1IDset.add(i);
            }

            while (printed < this.scaleTableSize.get(curTable)) {
                System.out.println("In Sufficient Tuple--Adding " + (printed - this.scaleTableSize.get(curTable)));
                Collections.shuffle(fk1IDset);
                for (long i = 0; i < fk1IDset.size() && printed < this.scaleTableSize.get(curTable); i++) {
                    long j = (long) (scaledSourceTableSize[1] * Math.random());
                    long newid = scaledSourceTableSize[0] * i + j;
                    if (!usedId.contains(newid)) {
                        pw.write("" + currentID);
                        pw.write(delimiter + i + delimiter + j);
                        int matchId = (int) Math.floor(this.originalDB.tables[tableID].nonKeys.length * Math.random());
                        if (this.originalDB.tables[tableID].nonKeys[matchId] != null) {
                            pw.write(delimiter + this.originalDB.tables[tableID].nonKeys[matchId]);
                        }
                        pw.newLine();
                        currentID++;

                        usedId.add(newid);
                        printed++;
                    }
                }
            }
        }
    }

    /**
     * 
     * @param originalReverseRVDistribution
     * @param scaleTableSize
     * @param outPath
     * @param originalDB
     * @param delimiter 
     */
    public void setInitials(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseRVDistribution, HashMap<String, Integer> scaleTableSize, String outPath, DB originalDB, String delimiter) {
        mergedDegreeTitle = originalDB.mergedDegreeTitle;
        this.originalReverseRVDistribution = originalReverseRVDistribution;
        this.scaleTableSize = scaleTableSize;
        this.outPath = outPath;
        this.originalDB = originalDB;
        this.referencingTable = originalDB.fkRelation;
        this.delimiter = delimiter;
    }
    
    /**
     * Calculate CK appearing index
     * @param tableID
     * @return 
     */
    private int[] calculateFKIndexOfJD(int tableID) {
        String curTable = originalDB.getTableName(tableID);
        int[] fkIndexOfJD = new int[referencingTable.get(curTable).size()];

        for (int i = 0; i < fkIndexOfJD.length; i++) {
            ComKey ck = referencingTable.get(curTable).get(i);
            fkIndexOfJD[i] = this.mergedDegreeTitle.get(ck.sourceTable).indexOf(ck);
        }
        return fkIndexOfJD;
    }
    
    
    /**
     * 
     * @param curTable
     * @return size of the scaled tables
     */
    private int[] calculateScaledTableSize(String curTable) {
        int[] scaledSourceTableSize = new int[referencingTable.get(curTable).size()];
        if (referencingTable.get(curTable).size() == 2) {
            for (int i = 0; i < referencingTable.get(curTable).size(); i++) {
                String s = referencingTable.get(curTable).get(i).sourceTable;
                scaledSourceTableSize[i] = this.scaleTableSize.get(s);
            }
        }
        return scaledSourceTableSize;
    }

    /**
     *
     * @param sum
     * @return closestRV having the sum
     */
    private ArrayList<ArrayList<Integer>> extractRVsBasedOnSum(int sum) {
        int size = originalRVSumMap.get(sum).size();
        if (size == 0) {
            originalRVSumMap.remove(sum);
        } else {
            int candidateIndex = rand.nextInt(size);
            return originalRVSumMap.get(sum).get(candidateIndex);
        }
        return new ArrayList<>();
    }
    
    /**
     * Cleaning job
     * @param curTable
     * @param closestRV 
     */
    private void cleanRVForSumMap(String curTable, ArrayList<ArrayList<Integer>> closestRV) {
        originalReverseRVDistribution.get(curTable).remove(closestRV);
        int rvDegreeSum = calculateSum(closestRV);
        originalRVSumMap.get(rvDegreeSum).remove(closestRV);

        if (originalRVSumMap.get(rvDegreeSum).isEmpty()) {
            originalRVSumMap.remove(rvDegreeSum);
        }
    }

}
