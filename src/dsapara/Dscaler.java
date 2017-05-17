/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dsapara.paraComputation.ParaJoinTableGen;
import dsapara.paraComputation.ParaMatchKV;
import dsapara.paraComputation.ParaKeyIdAssign;
import dsapara.paraComputation.ParaCompJDSum;
import dsapara.paraComputation.ParaIdAssign;
import dsapara.paraComputation.ParaSort;
import dsapara.paraComputation.ParaCompAvaStats;
import dscaler.dataStruct.AvaliableStatistics;
import dbstrcture.Configuration;
import dscaler.dataStruct.CoDa;
import dbstrcture.DB;
import dbstrcture.ComKey;
import dsapara.paraComputation.ParaRVCorr;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import static org.kohsuke.args4j.ExampleMode.ALL;
import org.kohsuke.args4j.Option;

/**
 *
 * @author workshop
 */
public class Dscaler {

    /**
     * @param args the command line arguments
     */
    int scaledVertexSize = 0;
       String twoRef = "socialgraph";
    final String scaleTableStr = "scaleTable.txt";
    public boolean eveNum = false;
    DB originalDB = new DB();
    CoDa originalCoDa = new CoDa();
    CoDa scaledCoda = new CoDa();

    @Option(name = "-i", usage = "input of the folder", metaVar = "INPUT")
    private String filePath = "";

    @Option(name = "-o", usage = "ouput of the folder", metaVar = "OUTPUT")
    private String outPath = "D:\\Research\\DATA\\dscaler\\acmdl\\out\\";

    @Option(name = "-d", usage = "Delimilter of the fields", metaVar = "MODE")
    private String delimiter  = "\\s+";

    @Option(name = "-f", usage = "Ignore the first line", metaVar = "Thread")
    private boolean ignoreFirst = false;

    @Option(name = "-static", usage = "Static scale of the database", metaVar = "StaticScale")
    private double staticS = 2;

    @Option(name = "-dynamic", usage = "Dynamic scale of the database, input is the file", metaVar = "DynamicScale")
    private String dynamicSFile = "";

    @Option(name = "-l", usage = "leading index of the fks", metaVar = "Leading Index")
    private int leading = 0;

    private HashMap<String, Integer> loadSaleTable() throws FileNotFoundException {

        HashMap<String, Integer> map = new HashMap<>();
        if (this.dynamicSFile.length() > 0) {
            Scanner scanner = new Scanner(new File(filePath + "/" + this.scaleTableStr));

            while (scanner.hasNext()) {
                String[] splits = scanner.nextLine().trim().split("\\s+");
                //System.out.println(splits.toString());
                map.put(splits[1].trim(), Integer.parseInt(splits[0].trim()));
            }
        } else {
            for (Entry<String, Integer> entry : this.originalDB.tableSize.entrySet()) {
                map.put(entry.getKey(), (int) (entry.getValue() * this.staticS));
            }
        }
        return map;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Dscaler fsq = new Dscaler();
        fsq.parseParameter(args);
       // fsq.run();
    }

    private void parseParameter(String[] args) throws FileNotFoundException {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            if (!filePath.isEmpty() && !outPath.isEmpty()) {
                Dscaler fsq = new Dscaler();
                if (delimiter.equals("t")){
                    delimiter = "\t";
                }
                fsq.delimiter  = delimiter;
                fsq.ignoreFirst = ignoreFirst;
                fsq.dynamicSFile = dynamicSFile;
                fsq.filePath = filePath;
                fsq.outPath = outPath;
                fsq.staticS = staticS;
                try {
                    fsq.run();
                } catch (IOException ex) {
                    Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        } catch (CmdLineException e) {
            System.err.println("  Example: java SampleMain" + parser.printExample(ALL));
            return;
        }
    }

    
    public Dscaler(String string) {
        filePath = string;
    }

    Dscaler() {
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

    HashMap<String, Integer> scaleCounts = new HashMap<>();
    HashMap<String, Integer> scaleTableSize = new HashMap<>();

    private void scaleDistribution()
            throws FileNotFoundException {
        ArrayList<Thread> arr = new ArrayList<>();
        int min = Integer.MAX_VALUE;
           for (Entry<ComKey, HashMap<Integer, Integer>> entry : this.originalCoDa.idFreqDis.entrySet()) {
            min = Math.min(min,entry.getValue().size());
           }
        for (Entry<ComKey, HashMap<Integer, Integer>> entry : this.originalCoDa.idFreqDis.entrySet()) {
            ComKey key = entry.getKey();
            String sourceTable = key.sourceTable;
            String dependTable = key.referencingTable;
            DegreeScaler degreeScaler = new DegreeScaler(this.scaledCoda.idFreqDis, scaleCounts, key, entry.getValue());
            
            degreeScaler.s = 1.0 * this.scaleTableSize.get(sourceTable) * 1.0 / originalDB.tableSize.get(sourceTable);;
            degreeScaler.dependAfter = this.scaleTableSize.get(dependTable);
            degreeScaler.sourceAfter = this.scaleTableSize.get(sourceTable);
            if (min < 50){
                degreeScaler.run();
            } else {
                Thread thr = new Thread(degreeScaler);
                arr.add(thr);
            }
        }
        
        
        
        for (Thread thr : arr) {
            thr.start();
        }
        for (Thread thr : arr) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                System.err.println("ex: " +ex);
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
       
      //  System.err.println("scaledCoda.idFreqDis: " + scaledCoda.idFreqDis + "\t" + originalCoDa.idFreqDis);
    }

    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> correlateRV(HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalRVDis,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle,
            HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts, HashMap<String, Boolean> uniqueNess,
            HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap,
            HashMap<String, ArrayList<ComKey>> referenceTable
    ) {

        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDis = new HashMap<>();

        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> originalRVEntry : originalRVDis.entrySet()) {
            String curTable = originalRVEntry.getKey();

            if (eveNum) {
                for (Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV : originalRVEntry.getValue().entrySet()) {
                    ArrayList<ArrayList<Integer>> reverseKey = new ArrayList<>();
                    reverseKey.add(originalRV.getKey().get(1));
                    reverseKey.add(originalRV.getKey().get(0));
                    if (!originalRVEntry.getValue().containsKey(reverseKey) || !originalRVEntry.getValue().get(reverseKey).equals(originalRV.getValue())) {
                        System.exit(-1);
                    }
                }

            }
            ParaRVCorr paraRVCorr = new ParaRVCorr(jdSumMap, scaledRVDis, ckJDAvaCounts, originalRVEntry.getValue(), 
                    scaledJDDis,  mergedDegreeTitle);

            paraRVCorr.originalCoDa = this.originalCoDa;
            paraRVCorr.eveNum = eveNum;
            paraRVCorr.sRatio = this.scaleTableSize.get(curTable) * 1.0 / this.originalDB.tableSize.get(curTable);
            paraRVCorr.curTable = curTable;
            paraRVCorr.referenceTable = referenceTable;
            paraRVCorr.mappedBestJointDegree = this.mappedBestJointDegree;
            paraRVCorr.uniqueNess = uniqueNess;
            Thread thread = new Thread(paraRVCorr);
            threadList.add(thread);
            thread.start();
            eveNum = false;
        }

        for (Thread thr : threadList) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return scaledRVDis;
    }

    public void run() throws FileNotFoundException, IOException {
        originalDB.loadMap(filePath + Configuration.configFile);
        System.out.println("====================Map Loaded=============================");
        originalDB.load_fkRelation();
         originalDB.processMergeDegreeTitle();
        originalDB.loadTuple(filePath, leading, ignoreFirst, delimiter );
        if (delimiter.equals("\\s+")){
            delimiter = "\t";
        }
        this.scaleTableSize = loadSaleTable();
        System.gc();
        System.out.println("\n===============================Extract ID Counts====================");
        originalCoDa.loadKeyCounts(originalDB.fkRelation, originalDB);

        System.out.println("======================Compute Joint Degree=====================");
        originalCoDa.processJointDegreeTable(originalDB.chainKey_map.keySet());
        originalCoDa.processJointDegree(originalDB);

        System.out.println("===========================Process correlation=================");
        originalCoDa.processRV(originalDB);
        System.out.println("====================Process Key Distribution =====================");
        originalCoDa.processKV();

        System.out.println("======================Process FreCounts====================");
        originalCoDa.processIdFrequency();
        originalCoDa.processJointDis();
        //HashMap<String, ArrayList<ComKey>> referencingTables = this.referencingTable;

        long start = System.currentTimeMillis();
        System.out.println("====================Scale Distribution======================= ");
        scaleDistribution();
        originalCoDa.dropIdFreqDis();
        originalDB.dropFKs();

        System.out.println("====================Joint Degree Correlation======================");
        corrMap();
        scaledCoda.dropIdFreqDis();

        System.out.println("====================Computation=======================");
        HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts = new HashMap<>();
        HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats = new HashMap<>();
        compAvaliableStats(srcJDAvaStats, ckJDAvaCounts);
        HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap = new HashMap<>();
        compJDSum(jdSumMap, srcJDAvaStats);

        System.err.println("==============================RVC====================================================");
        scaledCoda.rvDis = correlateRV(scaledCoda.jointDegreeDis, originalCoDa.rvDis,
                originalDB.mergedDegreeTitle,  ckJDAvaCounts,
                convertUniqueness(originalDB.tableType), jdSumMap, originalDB.fkRelation);
        
        ArrayList<String> keyTables = new ArrayList<>();
        ArrayList<String> refTables = new ArrayList<>();
        ArrayList<String> joinTables = new ArrayList<>();
       
        computeKeyRefTables(keyTables, refTables,joinTables, srcJDAvaStats);
      
        
        System.out.println("====================Print Pure Join Tables==========================");
        localequatingMergedIDs(srcJDAvaStats, this.originalCoDa.reverseJointDegrees, joinTables);

        System.out.println("====================Assign IDs To RV/KV==========================");
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs = new HashMap<>(); //KEY IS THE DEGREE, VALUE ARE THOSE IDS WITH THE DEGREE
        HashMap<String, ArrayList<ArrayList<Integer>>> rvFKids
                = assignReferenceTable(this.scaledCoda.rvDis, referencingIDs, srcJDAvaStats, this.originalCoDa.reverseRVs);
        
        System.out.println("================Matching RV & JD id For KV ======================");
        HashMap<String, HashMap<Integer, Integer>> scaledBiMap = new HashMap<>();
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs = new HashMap<>();

        if (!this.originalCoDa.kvDis.isEmpty()) {
            scaledBiMap = matchBiDegree(referencingIDs, srcJDAvaStats, this.originalCoDa.kvDis, keyVectorIDs, keyTables);
        }
        this.originalCoDa.dropKVDis(); 
        System.out.println("====================Localized Key IDs==========================");
        HashMap<String, HashMap<Integer, Integer>> localkeyMaps = localequatingIDs(keyVectorIDs, 
                this.originalCoDa.reverseKV, keyTables);
        keyVectorIDs = null;
        //reverseKeyDistribution = null;
        this.originalCoDa.reverseKV = null;
        localKeyIDs(rvFKids, localkeyMaps, scaledBiMap, keyTables, "local");
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

    private void compAvaliableStats(
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats,
            HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts) {

        ArrayList<Thread> threadList = new ArrayList<>();
         int min = Integer.MAX_VALUE;
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry : scaledCoda.jointDegreeDis.entrySet()) {
            min = Math.min(min, jointDegreeEntry.getValue().size());
        }
        //System.err.println("this.scaledCoda.jointDegreeDis: " + this.scaledCoda.jointDegreeDis);
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeentry : this.scaledCoda.jointDegreeDis.entrySet()) {
            ParaCompAvaStats ppad = new ParaCompAvaStats(jointDegreeentry, srcJDAvaStats, ckJDAvaCounts);
            if (min < 50){
                ppad.run();
            } else{
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

    int indexcount = 0;

    private HashMap<String, HashMap<Integer, Integer>> localequatingIDs(
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseKV,
            ArrayList<String> refTables) {
        HashMap<String, HashMap<Integer, Integer>> result = new HashMap<>();
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> kvIDentry : keyVectorIDs.entrySet()) {
            if (!eveNum && refTables.contains(kvIDentry.getKey())) {
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
    boolean staticUp = true;

    String corrFile = "corrFile.txt";
    String mergeFile = "mergeFile.txt";
    String reverseMerge = "reverseMerge.txt";
    String reverseCorr = "reverseCorr.txt";
    String scaledCorr = "scaledCorrFile.txt";
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree = new HashMap<>();

    private void corrMap() {
        ArrayList<Thread> threadList = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry : originalCoDa.jointDegreeDis.entrySet()) {
            min = Math.min(min, jointDegreeEntry.getValue().size());
        }
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> jointDegreeEntry : originalCoDa.jointDegreeDis.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> scaledIdFreqs = new ArrayList<>();
            for (ComKey ck : jointDegreeEntry.getKey()) {
                scaledIdFreqs.add(scaledCoda.idFreqDis.get(ck));
            }
            String sourceTable = jointDegreeEntry.getKey().get(0).sourceTable;
            JDCorrelation jdCorrelation = new JDCorrelation(
                    this.scaledCoda.jointDegreeDis, jointDegreeEntry.getKey(), scaledIdFreqs, jointDegreeEntry.getValue());
            jdCorrelation.initialize(
                    scaleTableSize.get(sourceTable) * 1.0 / originalDB.tableSize.get(sourceTable), mappedBestJointDegree);
            if (min < 50){
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

    private void compJDSum(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats
    ) {
        ArrayList<Thread> liss = new ArrayList<>();
        //System.err.println("originalDB.mergedDegreeTitle: "+originalDB.mergedDegreeTitle);
        for (Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStatsEntry : srcJDAvaStats.entrySet()) {
            for (ComKey ck : originalDB.mergedDegreeTitle.get(srcJDAvaStatsEntry.getKey())) {
                ParaCompJDSum pdc = new ParaCompJDSum(distanceMap, srcJDAvaStatsEntry, ck);
                pdc.singlerun();
            }
        }

        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    ArrayList<Integer> curIndexes;
    //HashMap<String, List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>> sortedCorrs = new HashMap<>();

    private void localKeyIDs(HashMap<String, ArrayList<ArrayList<Integer>>> assignIDs,
            HashMap<String, HashMap<Integer, Integer>> localkeyMaps,
            //  HashMap<String, HashMap<String, ArrayList<String>>> tuples, 
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
                int tableNum = this.originalDB.getTableNum(entry.getKey());
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
                // tuples.remove(jointDegreeEntry.getKey());

              //  System.out.println(count + " " + file + "   " + entry.getValue().size());

            }
        }
    }

    
    ArrayList<Thread> idthread = new ArrayList<>();

    private HashMap<String, ArrayList<ArrayList<Integer>>> assignReferenceTable(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> avaInfo,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseRVDistribution
    ) {
        HashMap<String, ArrayList<ArrayList<Integer>>> rvFKids = new HashMap<>();
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationEntry : scaledCorrelationDistribution.entrySet()) {
            if (!avaInfo.containsKey(scaledCorrelationEntry.getKey())) {
                ParaIdAssign paraIdAssign = new ParaIdAssign(scaledCorrelationEntry, scaledCorrelationDistribution,  avaInfo);
                paraIdAssign.mergedDegreeTitle = originalDB.mergedDegreeTitle;
                paraIdAssign.reverseRVDistribution = reverseRVDistribution;
                paraIdAssign.scaleTableSize = this.scaleTableSize;
                paraIdAssign.outPath = this.outPath;
                paraIdAssign.originalDB = this.originalDB;
                paraIdAssign.referencingTable = originalDB.fkRelation;
                paraIdAssign.delimiter = this.delimiter;
                Thread thread=  new Thread(paraIdAssign);
                idthread.add(thread);
                thread.start();
            } else {
                ParaKeyIdAssign paraKeyIdAssign = new ParaKeyIdAssign(rvFKids, scaledCorrelationEntry, scaledCorrelationDistribution, originalDB.fkRelation, referencingIDs, avaInfo);
                paraKeyIdAssign.mergedDegreeTitle = this.originalDB.mergedDegreeTitle;
                paraKeyIdAssign.reverseDistribution = reverseRVDistribution;
                paraKeyIdAssign.scaleTableSize = this.scaleTableSize;
                paraKeyIdAssign.delimiter = this.delimiter ;
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

    private void localequatingMergedIDs(
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseMergedDegree,
            ArrayList<String> joinTables) {
        ArrayList<Thread> threadList = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jdAvaEntry : srcJDAvaStats.entrySet()) {
            if (joinTables.contains(jdAvaEntry.getKey())) {
                ParaJoinTableGen joinTableGen = new ParaJoinTableGen();
                joinTableGen.outPath = this.outPath;
                joinTableGen.jdAvaEntry = jdAvaEntry;
                joinTableGen.reverseMergedDegree = reverseMergedDegree;
                joinTableGen.mergedDegreeTitle = this.originalDB.mergedDegreeTitle;
                joinTableGen.originalDB = this.originalDB;
                joinTableGen.delimiter = delimiter;
                Thread thread = new Thread(joinTableGen);
                threadList.add(thread);
                thread.start();

            }
        }

        return ;
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

    private void computeKeyRefTables(ArrayList<String> keyTables, ArrayList<String> refTables, ArrayList<String> joinTables, HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> srcJDAvaStats) {
      for (String entry : this.scaledCoda.rvDis.keySet()) {
            refTables.add(entry);
        }

        for (String table : srcJDAvaStats.keySet()) {
            if (refTables.contains(table)) {
                keyTables.add(table);
                refTables.remove(table);
            } else {
                joinTables.add(table);
            }
        }}

}
