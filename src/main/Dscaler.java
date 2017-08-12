/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import paraComputation.ParaKVTableGeneration;
import db.structs.ComKey;
import db.structs.DB;
import db.structs.Options;
import paraComputation.ParaReferencedOnlyTableGeneration;
import paraComputation.ParaMapJDToRV;
import paraComputation.ParaReferencingOnlyTableFKPairing;
import paraComputation.ParaCalculateJDSum;
import paraComputation.ParaReferencingOnlyTableGeneration;
import paraComputation.ParaCompAvaStats;
import dataStructure.AvaliableStatistics;
import dataStructure.CoDa;
import paraComputation.ParaRVCorr;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */
public class Dscaler {

    /**
     * @param args the command line arguments
     */
    int scaledVertexSize = 0;
    final String scaleTableStr = "scaleTable.txt";
    DB originalDB = new DB();
    CoDa originalCoDa = new CoDa();
    CoDa scaledCoda = new CoDa();

    String delimiter;
    boolean ignoreFirst;
    String dynamicSFile;
    String filePath;
    String outPath;
    double staticS;
    int leading;

    ArrayList<Integer> curIndexes;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMapping = new HashMap<>();
    HashMap<String, Integer> scaleCounts = new HashMap<>();
    HashMap<String, Integer> scaledTableSize = new HashMap<>();
    int indexcount = 0;
    ArrayList<Thread> idthread = new ArrayList<>();

    /**
     *
     * @return The ScaledTableSize Map
     * @throws FileNotFoundException
     */
    private HashMap<String, Integer> loadSaleTable() throws FileNotFoundException {

        HashMap<String, Integer> scaledTableSize = new HashMap<>();
        if (this.dynamicSFile.length() > 0) {
            Scanner scanner = new Scanner(new File(filePath + "/" + this.scaleTableStr));

            while (scanner.hasNext()) {
                String[] splits = scanner.nextLine().trim().split("\\s+");
                scaledTableSize.put(splits[1].trim(), Integer.parseInt(splits[0].trim()));
            }
        } else {
            for (String table : originalDB.getAllTables()) {
                int size = originalDB.getTableSize(table);
                scaledTableSize.put(table, (int) (size * this.staticS));
            }
        }

        return scaledTableSize;
    }

    /**
     * Scale the degree distribution in parallel.
     */
    private void scaleDistribution() {
        ArrayList<Thread> threadList = new ArrayList<>();

        for (Entry<ComKey, int[]> entry : this.originalCoDa.fkIDCounts.entrySet()) {
            String sourceTable = entry.getKey().getSourceTable();
            String referencingTable = entry.getKey().getReferencingTable();
            DegreeScaling degreeScale = new DegreeScaling();
            degreeScale.setInitials(originalCoDa.idDegreeDistribution.get(entry.getKey()), scaledTableSize.get(referencingTable), scaledTableSize.get(sourceTable),
                    1.0 * this.scaledTableSize.get(sourceTable) * 1.0 / originalDB.tableSize.get(sourceTable), entry.getKey(), scaledCoda.idDegreeDistribution);
            Thread thread = new Thread(degreeScale);
            threadList.add(thread);
            thread.start();
        }

        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    /**
     * Correlate RV
     *
     * @param scaledJDDis
     * @param originalRVDis
     * @param mergedDegreeTitle
     * @param jointDegreeAvaStats
     * @param uniqueNess
     * @param jdSumMap
     * @param referencingTableMap
     * @return rvDistribution
     */
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> referencingVectorCorrelation(
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalRVDis,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<String, Boolean> uniqueNess,
            HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap,
            HashMap<String, ArrayList<ComKey>> referencingTableMap,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> originalRVMappedScaledRV, ArrayList<String> kvTables,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledRVMappedToOriginalRV
    ) {

        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDistribution = new HashMap<>();

        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalRVEntry : originalRVDis.entrySet()) {
            String curTable = originalRVEntry.getKey();
            System.err.println("sclaedSize: " + scaledTableSize.get(curTable));
            //boolean kvTable = kvTables.contains(curTable);
            int saveMapFlag = -1;
            int tableID = originalDB.getTableID(curTable);
            if (originalDB.getTableNonKeyString(tableID)[0] != null && !kvTables.contains(curTable)) {
                saveMapFlag = 1;
            } else if (kvTables.contains(curTable)) {
                saveMapFlag = 0;
            }
            ParaRVCorr paraRVCorr = new ParaRVCorr(jdSumMap, scaledRVDistribution, jointDegreeAvaStats, originalRVEntry.getValue(),
                    scaledJDDis, mergedDegreeTitle);
            ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList
                    = calculateNorm1JointDegreeMap(curTable, referencingTableMap, mergedDegreeTitle);
            paraRVCorr.setInitials(saveMapFlag, originalCoDa, this.scaledTableSize.get(curTable) * 1.0 / this.originalDB.tableSize.get(curTable),
                    curTable, referencingTableMap, norm1JointDegreeMappingList, uniqueNess, originalRVMappedScaledRV, scaledRVMappedToOriginalRV);
            //paraRVCorr.run();
            Thread thread = new Thread(paraRVCorr);
            threadList.add(thread);
            thread.start();

        }

        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return scaledRVDistribution;
    }

    public void run() throws FileNotFoundException, IOException {

        System.out.println("======================Load Text Into Memory====================");
        originalDB.initial_loading(filePath, leading, ignoreFirst, delimiter, Options.loadFK, ".txt", filePath + Configuration.configFile);
        scaledTableSize = loadSaleTable();
        System.gc();

        System.out.println("======================Extract ID Counts====================");
        originalCoDa.loadFKIDCounts(originalDB);
        originalCoDa.calculateDegreeDistribution();

        System.out.println("======================Calculate Joint Degree====================");
        originalCoDa.calculateJointDegrees(originalDB);

        System.out.println("======================Calculate RV correlation====================");
        originalCoDa.calculateRV(originalDB);

        System.out.println("======================Calculate KV  Distribution=====================");
        originalCoDa.calculateKV();

        System.out.println("======================Scale Degree Distribution=====================");
        scaleDistribution();
        originalCoDa.dropIdFreqDis();
        originalDB.dropFKs();
        System.gc();

        System.out.println("======================Joint Degree Correlation=====================");
        jointDegreeDistributionSynthesis();

        System.out.println("======================Preparation For RVC=====================");
        HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats = new HashMap<>();
        calAvaStatsForJointDegree(jointDegreeAvaStats);
        HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jointDegreeSumMap = new HashMap<>();
        calculateJointDegreeSumMap(jointDegreeSumMap, jointDegreeAvaStats);

        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> originalRVMappedScaledRV = new HashMap<>();
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledRVMappedtoOriginalRV = new HashMap<>();
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledKVMappedtoOriginalKV = new HashMap<>();

        ArrayList<String> kvTables = new ArrayList<>();
        ArrayList<String> referencingOnlyTables = new ArrayList<>();
        ArrayList<String> referencedOnlyTables = new ArrayList<>();

        partitionTables(kvTables, referencingOnlyTables, referencedOnlyTables, jointDegreeAvaStats);
        System.err.println("======================RVC=====================");
        scaledCoda.rvDistribution = referencingVectorCorrelation(scaledCoda.jointDegreeDistribution, originalCoDa.rvDistribution,
                originalDB.mergedDegreeTitle, jointDegreeAvaStats,
                convertUniqueness(originalDB.tableType), jointDegreeSumMap, originalDB.fkRelation, originalRVMappedScaledRV, kvTables, scaledRVMappedtoOriginalRV);

        System.out.println("======================Generate Referenced Only Tables=====================");
        generateReferencedOnlyTables(jointDegreeAvaStats, this.originalCoDa.reverseJointDegrees, referencedOnlyTables);

        System.out.println("======================Synthesize Tuples For RV Related Tables=====================");
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKIDs = new HashMap<>(); //KEY IS THE DEGREE, VALUE ARE THOSE IDS WITH THE DEGREE
        int[][][] scaledRVFKIDs
                = synthesizeRVRelatedTables(this.scaledCoda.rvDistribution, scaledRVPKIDs, jointDegreeAvaStats,
                        this.originalCoDa.reverseRVs, scaledRVMappedtoOriginalRV);
        System.err.println("kvTavkes: " + kvTables);

        System.out.println("======================Synthesize KV IDs=====================");
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV = new HashMap<>();
        HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap = mapJDIDTORVID(scaledKVMappedtoOriginalKV, originalRVMappedScaledRV, scaledRVPKIDs, jointDegreeAvaStats, originalCoDa.kvDistribution,
                scaledReverseKV, kvTables);

        this.originalCoDa.dropKVDis();
        System.out.println("======================Generate KV Tables=====================");
        generateKVTables(scaledReverseKV,
                this.originalCoDa.reverseKV, kvTables, scaledJDIDToRVIDMap, scaledRVFKIDs, scaledKVMappedtoOriginalKV);

    }

    /**
     * This method calculates the avaliable statistics for the scaled
     * joint-degrees
     *
     * @param jointDegreeAvaStats
     */
    private void calAvaStatsForJointDegree(
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats
    ) {

        ArrayList<Thread> threadList = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry : scaledCoda.jointDegreeDistribution.entrySet()) {
            min = Math.min(min, jointDegreeEntry.getValue().size());
        }

        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeentry : this.scaledCoda.jointDegreeDistribution.entrySet()) {
            ParaCompAvaStats ppad = new ParaCompAvaStats(jointDegreeentry, jointDegreeAvaStats);
            if (min < 50) {
                ppad.run();
            } else {
                Thread thr = new Thread(ppad);
                threadList.add(thr);
                thr.start();
            }
        }
        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    /**
     * Generate KV Tables
     *
     * @param scaledReverseKV
     * @param originalReverseKV
     * @param kvTables
     * @param scaledJDIDToRVIDMap
     * @param scaledRVFKIDs
     */
    private void generateKVTables(
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseKV,
            ArrayList<String> kvTables,
            HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap,
            int[][][] scaledRVFKIDs,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledKVMappedtoOriginalKV
    ) {
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledKVIDentry : scaledReverseKV.entrySet()) {
            if (kvTables.contains(scaledKVIDentry.getKey())) {
                ParaKVTableGeneration kvTableGeneration = new ParaKVTableGeneration();

                kvTableGeneration.setInitials(scaledKVMappedtoOriginalKV, originalReverseKV, scaledKVIDentry, scaledJDIDToRVIDMap, scaledRVFKIDs, originalDB, outPath, delimiter);
                Thread thread = new Thread(kvTableGeneration);
                threadList.add(thread);
                thread.start();
            }
        }

        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return;
    }

    /**
     * Synthesize joint degree distribution.
     */
    private void jointDegreeDistributionSynthesis() {
        ArrayList<Thread> threadList = new ArrayList<>();
        int minEntryNumber = extractMinEntryNumber();

        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> jointDegreeEntry : originalCoDa.reverseJointDegrees.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions = scaledCoda.extractDegreeDistributions(jointDegreeEntry.getKey());

            String sourceTable = jointDegreeEntry.getKey().get(0).getSourceTable();
            System.out.print(sourceTable);
            JDCorrelation jdCorrelation = new JDCorrelation(
                    this.scaledCoda.jointDegreeDistribution, jointDegreeEntry.getKey(), scaledDegreeDistributions, jointDegreeEntry.getValue());
            jdCorrelation.initialize(scaledTableSize.get(sourceTable) * 1.0 / originalDB.tableSize.get(sourceTable), norm1JointDegreeMapping);

            if (minEntryNumber < 50) {
                jdCorrelation.run();
            } else {
                Thread thread = new Thread(jdCorrelation);
                threadList.add(thread);
            }
        }

        for (Thread thr : threadList) {
            thr.start();
        }

        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Calculate the joint degree sum for original distributions.
     *
     * @param jointDegreeSumMap
     * @param jointDegreeAvaStats
     */
    private void calculateJointDegreeSumMap(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jointDegreeSumMap,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats
    ) {
        for (Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStatsEntry : jointDegreeAvaStats.entrySet()) {
            for (ComKey ck : originalDB.mergedDegreeTitle.get(jointDegreeAvaStatsEntry.getKey())) {
                ParaCalculateJDSum pdc = new ParaCalculateJDSum(jointDegreeSumMap, jointDegreeAvaStatsEntry, ck);
                pdc.singlerun();
            }
        }
    }

    /**
     * Synthesize RV Related tables
     *
     * @param scaledRVDistribution
     * @param rvPKIDs
     * @param jointDegreeAvaStats
     * @param originalReverseRVDistribution
     * @return rvFKIDs
     */
    private int[][][] synthesizeRVRelatedTables(
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDistribution,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> rvPKIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> originalReverseRVDistribution,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledRVMappedtoOriginalRV
    ) {
        int[][][] rvFKIDs = new int[originalDB.getNumberOfTables()][][];
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVEntry : scaledRVDistribution.entrySet()) {
            if (!jointDegreeAvaStats.containsKey(scaledRVEntry.getKey())) {
                ParaReferencingOnlyTableGeneration referencingOnlyTableGen = new ParaReferencingOnlyTableGeneration(
                        scaledRVEntry, jointDegreeAvaStats);
                referencingOnlyTableGen.setInitials(scaledRVMappedtoOriginalRV, originalReverseRVDistribution, scaledTableSize, outPath, originalDB, delimiter);

                Thread thread = new Thread(referencingOnlyTableGen);
                thread.start();
            } else {
                ParaReferencingOnlyTableFKPairing paraReferencingOnlyTableFKPairing = new ParaReferencingOnlyTableFKPairing();
                paraReferencingOnlyTableFKPairing.setInitials(originalDB, jointDegreeAvaStats, scaledRVEntry, rvFKIDs, this.originalDB.mergedDegreeTitle,
                        scaledTableSize, rvPKIDs);

                Thread thread = new Thread(paraReferencingOnlyTableFKPairing);
                threadList.add(thread);
                thread.start();;
            }

        }
        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return rvFKIDs;
    }

    private HashMap<String, HashMap<Integer, Integer>> mapJDIDTORVID(
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> scaledKVMappedtoOriginalKV,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> originalRVMappedScaledRV,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDistribution,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledReverseKV, ArrayList<String> keyTables) {

        HashMap<String, HashMap<Integer, Integer>> scaledJDIDToRVIDMap = new HashMap<>();
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVPKentry : scaledRVPKIDs.entrySet()) {
            String curTable = scaledRVPKentry.getKey();
            if (keyTables.contains(curTable)) {
                int curTableID = originalDB.getTableID(curTable);
                boolean saveMap = originalDB.getTableNonKeyString(curTableID)[0] != null;

                ParaMapJDToRV paraMapJDToRV = new ParaMapJDToRV();
                paraMapJDToRV.setInitials(saveMap, scaledKVMappedtoOriginalKV, scaledJDIDToRVIDMap, this.scaledTableSize.get(curTable) * 1.0 / this.originalDB.tableSize.get(curTable),
                        jointDegreeAvaStats.get(curTable), scaledRVPKentry, scaledReverseKV, originalKVDistribution, originalRVMappedScaledRV.get(curTable), norm1JointDegreeMapping.get(originalDB.mergedDegreeTitle.get(curTable)));
                //paraMapJDToRV.run();
                Thread thread = new Thread(paraMapJDToRV);
                threadList.add(thread);
                thread.start();
            }
        }

        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return scaledJDIDToRVIDMap;
    }

    /**
     * Generates the referenced only tables
     *
     * @param jointDegreeAvaStats
     * @param origianlReverseJointDegrees
     * @param referencedOnlyTables
     */
    private void generateReferencedOnlyTables(
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> origianlReverseJointDegrees,
            ArrayList<String> referencedOnlyTables) {
        for (Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jdAvaEntry : jointDegreeAvaStats.entrySet()) {
            if (referencedOnlyTables.contains(jdAvaEntry.getKey())) {
                ParaReferencedOnlyTableGeneration joinTableGen = new ParaReferencedOnlyTableGeneration();
                joinTableGen.setInitials(outPath, jdAvaEntry, origianlReverseJointDegrees, originalDB.mergedDegreeTitle, originalDB, delimiter);

                Thread thread = new Thread(joinTableGen);
                thread.start();
            }
        }

        return;
    }

    private HashMap<String, Boolean> convertUniqueness(HashMap<String, String> tableType) {
        HashMap<String, Boolean> result = new HashMap<>();
        for (Entry<String, String> entry : tableType.entrySet()) {
            if (entry.getValue().equals("true")) {
                result.put(entry.getKey(), true);
            } else {
                result.put(entry.getKey(), false);
            }
        }
        return result;
    }

    /**
     * Partition the tables into KV, JD, RV
     *
     * @param kvTables
     * @param referencingTables
     * @param referencedTables
     * @param jointDegreeAvaStats
     */
    private void partitionTables(ArrayList<String> kvTables, ArrayList<String> referencingTables,
            ArrayList<String> referencedTables,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats) {

        for (String entry : this.originalCoDa.rvDistribution.keySet()) {
            referencingTables.add(entry);
        }

        for (String table : jointDegreeAvaStats.keySet()) {
            if (referencingTables.contains(table)) {
                kvTables.add(table);
                referencingTables.remove(table);
            } else {
                referencedTables.add(table);
            }
        }
    }

    void setInitials(String delimiter, boolean ignoreFirst, String dynamicSFile, String filePath, String outPath, double staticS, int leading) {
        this.delimiter = delimiter;
        this.ignoreFirst = ignoreFirst;
        this.dynamicSFile = dynamicSFile;
        this.filePath = filePath;
        this.outPath = outPath;
        this.staticS = staticS;
        this.leading = leading;
    }

    private int extractMinEntryNumber() {
        int min = Integer.MAX_VALUE;
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> jointDegreeEntry : originalCoDa.reverseJointDegrees.entrySet()) {
            min = Math.min(min, jointDegreeEntry.getValue().size());
        }
        return min;
    }

    private ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> calculateNorm1JointDegreeMap(
            String curTable, HashMap<String, ArrayList<ComKey>> referencingTableMap, HashMap<String, ArrayList<ComKey>> mergedDegreeTitle) {

        ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList = new ArrayList<>();
        for (ComKey referencingCK : referencingTableMap.get(curTable)) {
            HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> norm1JointDegreeMapClone = new HashMap<>();
            for (ArrayList<Integer> jd : norm1JointDegreeMapping.get(mergedDegreeTitle.get(referencingCK.getSourceTable())).keySet()) {
                norm1JointDegreeMapClone.put(jd, new ArrayList<ArrayList<Integer>>());
                for (ArrayList<Integer> mappedJD : norm1JointDegreeMapping.get(mergedDegreeTitle.get(referencingCK.getSourceTable())).get(jd)) {
                    norm1JointDegreeMapClone.get(jd).add(mappedJD);
                }
            }
            norm1JointDegreeMappingList.add(norm1JointDegreeMapClone);
        }
        return norm1JointDegreeMappingList;
    }

}
