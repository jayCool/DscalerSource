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
public class ParaReferencingOnlyTableFKPairing implements Runnable {

    public Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVEntry;
    public HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats;
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs;
    public DB originalDB;
    public HashMap<String, ArrayList<ComKey>> referencingTable;
    int currentID = 0;

    public String delimiter = "\t";
    int[][][] rvFKIDs;
    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> rvPKIDs = new HashMap<>();

    public HashMap<String, Integer> scaledTableSize;

    public void setInitials(DB originalDB, HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats, Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVEntry, int[][][] rvFKIDs, HashMap<String, ArrayList<ComKey>> mergedDegreeTitle, HashMap<String, Integer> scaledTableSize, String delimiter,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs) {
        this.jointDegreeAvaStats = jointDegreeAvaStats;
        this.scaledRVEntry = scaledRVEntry;
        this.rvFKIDs = rvFKIDs;
        this.mergedDegreeTitle = mergedDegreeTitle;
        this.scaledTableSize = scaledTableSize;
        this.delimiter = delimiter;
        this.referencingIDs = referencingIDs;
        this.originalDB = originalDB;
        this.referencingTable = originalDB.fkRelation;

    }

    @Override
    public void run() {
        String curTable = scaledRVEntry.getKey();
        int tableID = originalDB.getTableID(curTable);
        int tableSize = scaledTableSize.get(curTable);
        int numOfFKs = referencingTable.get(curTable).size();
        rvFKIDs[tableID] = new int[tableSize][numOfFKs];

        switch (originalDB.getReferencingTables(curTable).size()) {

            case 1:
                printTableForOneKey(tableID);
                break;
            case 2:
                generateTableForTwoKeys(tableID);
                break;
            default:
                generateTableForMoreKeys(tableID);
                break;
        }

    }

    /**
     * Generate table with only one FK
     *
     * @param tableID
     */
    private void printTableForOneKey(int tableID) {

        ComKey firstCK = referencingTable.get(scaledRVEntry.getKey()).get(0);
        int firstCKIndex = this.mergedDegreeTitle.get(firstCK.sourceTable).indexOf(firstCK);

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledRVFrequency : scaledRVEntry.getValue().entrySet()) {
            rvPKIDs.put(scaledRVFrequency.getKey(), new ArrayList<Integer>(scaledRVFrequency.getValue()));
            ArrayList<Integer> jointDegree = scaledRVFrequency.getKey().get(0);
            int frequency = scaledRVFrequency.getValue();

            int[] candidateIDs = jointDegreeAvaStats.get(firstCK.sourceTable).get(jointDegree).ids;

            generateSameAmountOfTuplesForCandidateIDs(tableID, frequency, candidateIDs, scaledRVFrequency.getKey());
            generateRemainderOfTuplesForCandidateIDs(firstCK, firstCKIndex, tableID, frequency, candidateIDs, jointDegree, scaledRVFrequency.getKey());
        }
    }

    /**
     * Generate table with only two FKs
     *
     * @param tableID
     * @return number of tuples printed
     */
    private int generateTableForTwoKeys(int tableID) {
        ComKey firstCK = referencingTable.get(scaledRVEntry.getKey()).get(0);
        ComKey secondCK = referencingTable.get(scaledRVEntry.getKey()).get(1);
        int firstCKIndex = this.mergedDegreeTitle.get(firstCK.getSourceTable()).indexOf(firstCK);
        int secondCKIndex = this.mergedDegreeTitle.get(secondCK.getSourceTable()).indexOf(secondCK);

        int printedNumber = 0;

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledRVFrequency : scaledRVEntry.getValue().entrySet()) {
            rvPKIDs.put(scaledRVFrequency.getKey(), new ArrayList<Integer>(scaledRVFrequency.getValue()));
            int frequency = scaledRVFrequency.getValue();
            printedNumber += frequency;

            int firstCKIdsLength = this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(scaledRVFrequency.getKey().get(0)).ids.length;
            int firstCKStartingIndex = this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(scaledRVFrequency.getKey().get(0)).startIndex[firstCKIndex];

            int[] firstFKIDFrequencs = new int[firstCKIdsLength];

            firstCKStartingIndex = calculateFirstFKIDFrequencies(frequency, firstCKIdsLength, firstFKIDFrequencs, firstCKStartingIndex);
            this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(scaledRVFrequency.getKey().get(0)).startIndex[firstCKIndex] = firstCKStartingIndex;

            int secondCKIdsLength = this.jointDegreeAvaStats.get(secondCK.getSourceTable()).get(scaledRVFrequency.getKey().get(1)).ids.length;
            int secondCKStartingIndex = this.jointDegreeAvaStats.get(secondCK.getSourceTable()).get(scaledRVFrequency.getKey().get(1)).startIndex[secondCKIndex];

            for (int firstCKIDIndex = 0; firstCKIDIndex < firstFKIDFrequencs.length; firstCKIDIndex++) {
                for (int idFrequency = 0; idFrequency < firstFKIDFrequencs[firstCKIDIndex]; idFrequency++) {
                    rvFKIDs[tableID][currentID][0] = jointDegreeAvaStats.get(firstCK.sourceTable).get(scaledRVFrequency.getKey().get(0)).ids[firstCKIDIndex];
                    rvFKIDs[tableID][currentID][1] = jointDegreeAvaStats.get(secondCK.sourceTable).get(scaledRVFrequency.getKey().get(1)).ids[secondCKStartingIndex];
                    rvPKIDs.get(scaledRVFrequency.getKey()).add(currentID);
                    secondCKStartingIndex++;
                    secondCKStartingIndex = secondCKStartingIndex % secondCKIdsLength;
                    currentID++;
                }
            }
            this.jointDegreeAvaStats.get(secondCK.sourceTable).get(scaledRVFrequency.getKey().get(1)).startIndex[secondCKIndex] = secondCKStartingIndex;
        }
        return printedNumber;
    }

    /**
     * Generate table with more than two FKs
     *
     * @param tableID
     */
    private void generateTableForMoreKeys(int tableID
    ) {
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> scaledRVFrequency : scaledRVEntry.getValue().entrySet()) {
            rvPKIDs.put(scaledRVFrequency.getKey(), new ArrayList<Integer>(scaledRVFrequency.getValue()));
            int[][] fkIDs = new int[scaledRVFrequency.getValue()][scaledRVFrequency.getKey().size()];
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
                    fkIDs[i][k] = jdID;
                }
                this.jointDegreeAvaStats.get(referencingTable.get(scaledRVEntry.getKey()).get(k).sourceTable).get(jointDegree).startIndex[fkTableIndexes[k]] = startingIDindex;
            }

            for (int i = 0; i < fkIDs.length; i++) {
                for (int k = 0; k < fkIDs[i].length; k++) {
                    rvFKIDs[tableID][currentID][k] = fkIDs[i][k];
                }
                rvPKIDs.get(scaledRVFrequency.getKey()).add(currentID);
                currentID++;
            }
        }

    }

    /**
     *
     * @param tableID
     * @param frequency
     * @param candidateIDs
     * @param rv
     * @throws IOException
     */
    private void generateSameAmountOfTuplesForCandidateIDs(int tableID, int frequency, int[] candidateIDs, ArrayList<ArrayList<Integer>> rv) {
        for (int i = 0; i < frequency / candidateIDs.length * candidateIDs.length; i++) {
            this.rvFKIDs[tableID][currentID][0] = candidateIDs[i % candidateIDs.length];
            rvPKIDs.get(rv).add(currentID);
            currentID++;
        }
    }

    /**
     * Generate the left over amount of tuples for the IDs with the same RV
     *
     * @param firstCK
     * @param firstCKIndex
     * @param tableID
     * @param frequency
     * @param candidateIDs
     * @param jointDegree
     * @param rv
     * @throws IOException
     */
    private void generateRemainderOfTuplesForCandidateIDs(ComKey firstCK, int firstCKIndex, int tableID, int frequency,
            int[] candidateIDs, ArrayList<Integer> jointDegree, ArrayList<ArrayList<Integer>> rv) {

        int firstCKStartingIndex = this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(jointDegree).startIndex[firstCKIndex];

        for (int i = frequency / candidateIDs.length * candidateIDs.length; i < frequency; i++) {
            rvFKIDs[tableID][currentID][0] = candidateIDs[firstCKStartingIndex];
            rvPKIDs.get(rv).add(currentID);
            firstCKStartingIndex++;
            firstCKStartingIndex = firstCKStartingIndex % candidateIDs.length;
            currentID++;
        }

        this.jointDegreeAvaStats.get(firstCK.getSourceTable()).get(jointDegree).startIndex[firstCKIndex] = firstCKStartingIndex;
    }

    /**
     * Calculate the FK appearing frequencies
     *
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
     * Calculate CK appearing index
     *
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

}
