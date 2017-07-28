/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import db.structs.ComKey;
import db.structs.DB;
import db.structs.Options;
import dsapara.paraComputation.ParaReferencedOnlyTableGeneration;
import dsapara.paraComputation.ParaMatchKV;
import dsapara.paraComputation.ParaKeyIdAssign;
import dsapara.paraComputation.ParaCalculateJDSum;
import dsapara.paraComputation.ParaIdAssign;
import dsapara.paraComputation.ParaCompAvaStats;
import dscaler.dataStruct.AvaliableStatistics;
import dbstrcture.Configuration;
import dscaler.dataStruct.CoDa;
import dsapara.paraComputation.ParaRVCorr;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
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
   // public boolean eveNum = false;
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
    HashMap<String, Integer> scaleTableSize = new HashMap<>();
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

    void processRaw() throws FileNotFoundException {
        String file = "checkins";
        Scanner scanner = new Scanner(new File(file + ".dat"));
        //scanner.nextLine();
        PrintWriter pw = new PrintWriter(file + ".txt");
        String[] values = scanner.nextLine().split("\\|");
        pw.print(values[0]);
        pw.print(values[1]);
        pw.print(values[2]);
        pw.print(values[5]);
        pw.println();
        scanner.nextLine();

        while (scanner.hasNext()) {
            values = scanner.nextLine().split("\\|");
            if (values.length >= 3) {
                pw.print(values[0]);
                pw.print(values[1]);
                pw.print(values[2]);
                pw.println(values[values.length - 1]);
            }
        }
        pw.close();
        scanner.close();

    }

  
    private void scaleDistribution() {
        for (Entry<ComKey, int[]> entry : this.originalCoDa.fkIDCounts.entrySet()) {
            String sourceTable = entry.getKey().getSourceTable();
            String referencingTable = entry.getKey().getReferencingTable();
            DegreeScaling degreeScale = new DegreeScaling();
            HashMap<Integer, Integer> scaledDis = degreeScale.scale(originalCoDa.idFreqDis.get(entry.getKey()), scaleTableSize.get(referencingTable), scaleTableSize.get(sourceTable),
                    1.0 * this.scaleTableSize.get(sourceTable) * 1.0 / originalDB.tableSize.get(sourceTable));
            scaledCoda.idFreqDis.put(entry.getKey(), scaledDis);
        }

    }

    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> referencingVectorCorrelation(
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalRVDis,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<String, Boolean> uniqueNess,
            HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap,
            HashMap<String, ArrayList<ComKey>> referencingTableMap
    ) {

        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDistribution = new HashMap<>();

        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalRVEntry : originalRVDis.entrySet()) {
            String curTable = originalRVEntry.getKey();

            ParaRVCorr paraRVCorr = new ParaRVCorr(jdSumMap, scaledRVDistribution, jointDegreeAvaStats, originalRVEntry.getValue(),
                    scaledJDDis, mergedDegreeTitle);
            paraRVCorr.setInitials(originalCoDa,this.scaleTableSize.get(curTable) * 1.0 / this.originalDB.tableSize.get(curTable),
                    curTable, referencingTableMap, norm1JointDegreeMapping, uniqueNess);
           
            
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
        originalDB.initial_loading(filePath, leading, ignoreFirst, delimiter, Options.loadFK, ".txt", filePath + Configuration.configFile);

        this.scaleTableSize = loadSaleTable();

        System.gc();

        System.out.println("\n===============================Extract ID Counts====================");
        originalCoDa.loadFKIDCounts(originalDB);

        System.out.println("======================Calculate Joint Degree=====================");
        originalCoDa.calculateJointDegrees(originalDB);

        System.out.println("===========================Calculate RV correlation=================");
        originalCoDa.calculateRV(originalDB);

        System.out.println("====================Calculate KV  Distribution =====================");
        originalCoDa.calculateKV();

        long start = System.currentTimeMillis();
        System.out.println("====================Scale Degree Distribution======================= ");
        originalCoDa.calculateDegreeDistribution();
        scaleDistribution();
        originalCoDa.dropIdFreqDis();

        System.out.println("======================Process FreCounts====================");
        originalDB.dropFKs();
        System.gc();
        System.out.println("====================Joint Degree Correlation======================");
        jointDegreeDistributionSynthesis();

        System.out.println("====================Preparation For RVC=======================");
        //HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts = new HashMap<>();
        HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats = new HashMap<>();
        calAvaStatsForJointDegree(jointDegreeAvaStats);
        HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jointDegreeSumMap = new HashMap<>();
        calculateJointDegreeSumMap(jointDegreeSumMap, jointDegreeAvaStats);

        System.err.println("==============================RVC====================================================");
        scaledCoda.rvDistribution = referencingVectorCorrelation(scaledCoda.jointDegreeDis, originalCoDa.rvDistribution,
                originalDB.mergedDegreeTitle, jointDegreeAvaStats,
                convertUniqueness(originalDB.tableType), jointDegreeSumMap, originalDB.fkRelation);

        ArrayList<String> kvTables = new ArrayList<>();
        ArrayList<String> referencingOnlyTables = new ArrayList<>();
        ArrayList<String> referencedOnlyTables = new ArrayList<>();

        partitionTables(kvTables, referencingOnlyTables, referencedOnlyTables, jointDegreeAvaStats);

        System.out.println("====================Generate Referenced Only Tables==========================");
        generateReferencedOnlyTables(jointDegreeAvaStats, this.originalCoDa.reverseJointDegrees, referencedOnlyTables);

        System.out.println("====================Assign IDs To RV/KV==========================");
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs = new HashMap<>(); //KEY IS THE DEGREE, VALUE ARE THOSE IDS WITH THE DEGREE
        HashMap<String, ArrayList<ArrayList<Integer>>> rvFKids
                = assignReferenceTable(this.scaledCoda.rvDistribution, referencingIDs, jointDegreeAvaStats, this.originalCoDa.reverseRVs);

        System.out.println("================Matching RV & JD id For KV ======================");
        HashMap<String, HashMap<Integer, Integer>> scaledBiMap = new HashMap<>();
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs = new HashMap<>();

        if (!this.originalCoDa.kvDis.isEmpty()) {
            scaledBiMap = matchBiDegree(referencingIDs, jointDegreeAvaStats, this.originalCoDa.kvDis, keyVectorIDs, kvTables);
        }
        this.originalCoDa.dropKVDis();
        System.out.println("====================Localized Key IDs==========================");
        HashMap<String, HashMap<Integer, Integer>> localkeyMaps = localequatingIDs(keyVectorIDs,
                this.originalCoDa.reverseKV, kvTables);
        keyVectorIDs = null;
        //reverseKeyDistribution = null;
        this.originalCoDa.reverseKV = null;
        localKeyIDs(rvFKids, localkeyMaps, scaledBiMap, kvTables, "local");
        scaledBiMap = null;
        localkeyMaps = null;

        for (Thread thr : this.idthread) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        long ended = System.currentTimeMillis();

        System.out.println("LocalRunning time=" + (ended - start - 0) / 1000 + "s");
        PrintWriter time = new PrintWriter(new BufferedWriter(new FileWriter("time.txt", true)));
        time.println(this.staticS + "    " + (ended - start - 0) / 1000);
        //more code
        time.close();
    }

    /**
     * This method calculates the avaliable statistics for the scaled joint-degrees
     * @param jointDegreeAvaStats 
     */
    private void calAvaStatsForJointDegree(
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats
           ) {

        ArrayList<Thread> threadList = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry : scaledCoda.jointDegreeDis.entrySet()) {
            min = Math.min(min, jointDegreeEntry.getValue().size());
        }
        
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeentry : this.scaledCoda.jointDegreeDis.entrySet()) {
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

   
    private HashMap<String, HashMap<Integer, Integer>> localequatingIDs(
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseKV,
            ArrayList<String> refTables) {
        HashMap<String, HashMap<Integer, Integer>> result = new HashMap<>();
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> kvIDentry : keyVectorIDs.entrySet()) {
            if ( refTables.contains(kvIDentry.getKey())) {
                LocalRefTableGen lft = new LocalRefTableGen();
                lft.result = result;
                lft.reverseKV = reverseKV;
                lft.kvIDentry = kvIDentry;
                Thread thr = new Thread(lft);
                liss.add(thr);
                thr.start();

            }
        }

        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return result;
    }
 
    private void jointDegreeDistributionSynthesis() {
        ArrayList<Thread> threadList = new ArrayList<>();
        int minEntryNumber = extractMinEntryNumber();

        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> jointDegreeEntry : originalCoDa.reverseJointDegrees.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions = scaledCoda.extractDegreeDistributions(jointDegreeEntry.getKey());

            String sourceTable = jointDegreeEntry.getKey().get(0).getSourceTable();
            
            JDCorrelation jdCorrelation = new JDCorrelation(
                    this.scaledCoda.jointDegreeDis, jointDegreeEntry.getKey(), scaledDegreeDistributions, jointDegreeEntry.getValue());
            jdCorrelation.initialize(scaleTableSize.get(sourceTable) * 1.0 / originalDB.tableSize.get(sourceTable), norm1JointDegreeMapping);
            
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

    
    private void localKeyIDs(HashMap<String, ArrayList<ArrayList<Integer>>> assignIDs,
            HashMap<String, HashMap<Integer, Integer>> localkeyMaps,
            HashMap<String, HashMap<Integer, Integer>> scaledBiMap,
            ArrayList<String> keyTables, String option) throws IOException {
        for (Entry<String, HashMap<Integer, Integer>> entry : localkeyMaps.entrySet()) {
            int count = 0;
            String curTable = entry.getKey();
            //System.out.println(entry.getKey());
            String refTable = "";
            for (String ref : assignIDs.keySet()) {
                if (ref.equals(curTable)) {
                    refTable = ref;
                }
            }
            //     int tableNum = originalDB.getTableNum(jointDegreeEntry.getKey());
            if (keyTables.contains(entry.getKey())) {
                File file = new File(outPath + "/" + entry.getKey() + ".txt");
                FileWriter writer = new FileWriter(file);
                BufferedWriter pw = new BufferedWriter(writer, 100000);
                int tableNum = this.originalDB.getTableID(entry.getKey());
                int size = this.originalDB.tables[tableNum].fkSize;
                for (Entry<Integer, Integer> entry2 : entry.getValue().entrySet()) {
                    pw.write("" + entry2.getKey());
                    int corId = scaledBiMap.get(entry.getKey()).get(entry2.getKey());
                    for (int t : assignIDs.get(refTable).get(corId)) {
                        pw.write(delimiter + t);
                    }
                    String rrid = "" + entry2.getValue();
                    int mappedPK = Integer.parseInt(rrid);
                    if (this.originalDB.tables[tableNum].nonKeys[mappedPK] != null) {
                        pw.write(delimiter + this.originalDB.tables[tableNum].nonKeys[mappedPK]);
                    }
                    pw.newLine();
                    count++;
                }

                pw.close();
            }
        }
    }

 
    private HashMap<String, ArrayList<ArrayList<Integer>>> assignReferenceTable(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> avaInfo,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseRVDistribution
    ) {
        HashMap<String, ArrayList<ArrayList<Integer>>> rvFKids = new HashMap<>();
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationEntry : scaledCorrelationDistribution.entrySet()) {
            if (!avaInfo.containsKey(scaledCorrelationEntry.getKey())) {
                ParaIdAssign paraIdAssign = new ParaIdAssign(scaledCorrelationEntry, scaledCorrelationDistribution, avaInfo);
                paraIdAssign.mergedDegreeTitle = originalDB.mergedDegreeTitle;
                paraIdAssign.reverseRVDistribution = reverseRVDistribution;
                paraIdAssign.scaleTableSize = this.scaleTableSize;
                paraIdAssign.outPath = this.outPath;
                paraIdAssign.originalDB = this.originalDB;
                paraIdAssign.referencingTable = originalDB.fkRelation;
                paraIdAssign.delimiter = this.delimiter;
                Thread thread = new Thread(paraIdAssign);
                idthread.add(thread);
                thread.start();
            } else {
                ParaKeyIdAssign paraKeyIdAssign = new ParaKeyIdAssign(rvFKids, scaledCorrelationEntry, scaledCorrelationDistribution, originalDB.fkRelation, referencingIDs, avaInfo);
                paraKeyIdAssign.mergedDegreeTitle = this.originalDB.mergedDegreeTitle;
                paraKeyIdAssign.reverseDistribution = reverseRVDistribution;
                paraKeyIdAssign.scaleTableSize = this.scaleTableSize;
                paraKeyIdAssign.delimiter = this.delimiter;
                Thread thread = new Thread(paraKeyIdAssign);
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

        return rvFKids;
    }

    private HashMap<String, HashMap<Integer, Integer>> matchBiDegree(
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalKVDis,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs, ArrayList<String> keyTables) {

        HashMap<String, HashMap<Integer, Integer>> scaledBiMap = new HashMap<>();
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> scaledRVentry : referencingIDs.entrySet()) {
            if (keyTables.contains(scaledRVentry.getKey())) {
                ParaMatchKV paraMatchKV = new ParaMatchKV();
                paraMatchKV.scaledBiMap = scaledBiMap;
                paraMatchKV.sRatio = this.scaleTableSize.get(scaledRVentry.getKey()) * 1.0 / this.originalDB.tableSize.get(scaledRVentry.getKey());
                paraMatchKV.srcJDAvaStats = srcJDAvaStats;
                paraMatchKV.scaledRVentry = scaledRVentry;
                paraMatchKV.keyVectorIDs = keyVectorIDs;
                paraMatchKV.originalKVDis = originalKVDis;
                Thread thread = new Thread(paraMatchKV);
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
        return scaledBiMap;
    }
    
    
    /**
     * Generates the referenced only tables
     * @param jointDegreeAvaStats
     * @param origianlReverseJointDegrees
     * @param referencedOnlyTables 
     */
    private void generateReferencedOnlyTables(
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> origianlReverseJointDegrees,
            ArrayList<String> referencedOnlyTables) {
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jdAvaEntry : jointDegreeAvaStats.entrySet()) {
            if (referencedOnlyTables.contains(jdAvaEntry.getKey())) {
                ParaReferencedOnlyTableGeneration joinTableGen = new ParaReferencedOnlyTableGeneration();
                joinTableGen.outPath = this.outPath;
                joinTableGen.jdAvaEntry = jdAvaEntry;
                joinTableGen.origianlReverseJointDegrees = origianlReverseJointDegrees;
                joinTableGen.mergedDegreeTitle = this.originalDB.mergedDegreeTitle;
                joinTableGen.originalDB = this.originalDB;
                joinTableGen.delimiter = delimiter;
                Thread thread = new Thread(joinTableGen);
                threadList.add(thread);
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

    private void partitionTables(ArrayList<String> kvTables, ArrayList<String> referencingTables, 
            ArrayList<String> referencedTables, 
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats) {
        
        for (String entry : this.scaledCoda.rvDistribution.keySet()) {
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

}
