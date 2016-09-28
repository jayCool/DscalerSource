 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.CoDa;
import dbstrcture.DB;
import dbstrcture.IdFeatures;
import dbstrcture.Table;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.text.html.parser.Entity;

/**
 *
 * @author workshop
 */
public class Dscaler {

    /**
     * @param args the command line arguments
     */
    String inputFile = "config.txt";
    String statistics = "subconfig.txt";
    int scaledVertexSize = 0;
    double ratioOfFixedP = 0.0003;
    double s = 2.5;
    public String corrOriginal = "pre29Dec/keyDegree.txt";
    public String filePath = "D:\\Research\\DATA\\dscaler\\acmdl\\";
    String delim = "\\s+";
    String twoRef = "socialgraph";
    String outFile = "D:\\Research\\DATA\\dscaler\\acmdl\\out\\";
    HashMap<String, ArrayList<ComKey>> referencingTable = new HashMap<>();
    boolean ignoreFirst = true;
    final String scaleTableStr = "scaleTable.txt";
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle = new HashMap<>();
    String dynamicAddr;
    DB originalDB = new DB();
    CoDa originalCoDa = new CoDa();
    CoDa scaledCoda = new CoDa();

    private HashMap<String, Integer> loadSaleTable() throws FileNotFoundException {

        HashMap<String, Integer> map = new HashMap<>();
        if (this.dynamicAddr.length() > 0) {
            Scanner scanner = new Scanner(new File(filePath + "/" + this.scaleTableStr));

            while (scanner.hasNext()) {
                String[] splits = scanner.nextLine().trim().split("\\s+");
                System.out.println(splits.toString());
                map.put(splits[1].trim(), Integer.parseInt(splits[0].trim()));
            }
        } else {
            for (Entry<String, Integer> entry : this.oldTableSize.entrySet()) {
                map.put(entry.getKey(), (int) (entry.getValue() * this.s));
            }
        }
        return map;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Dscaler fsq = new Dscaler();
        fsq.run();
    }
    private HashMap<String, Integer> oldTableSize = new HashMap<>();

    public Dscaler(String string) {
        filePath = string;
    }

    Dscaler() {
    }

    private void settleCorrVetex(MergeVertex corrVertex, String sourceTable) {
        corrVertex.corrOriginal = this.corrOriginal;
        corrVertex.scaledVertexSize = this.scaledVertexSize;
        //     System.out.println(this.scaledVertexSize+"vertexSize");
        corrVertex.stime = this.scaleTableSize.get(sourceTable) * 1.0 / this.oldTableSize.get(sourceTable);
        corrVertex.rationP = this.ratioOfFixedP;
        corrVertex.mappedBestJointDegree = this.mappedBestJointDegree;
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
    HashMap<String, Integer> tableSize = new HashMap<>();
    HashMap<String, Integer> tupleSize = new HashMap<>();
    HashMap<String, Boolean> uniqueNess = new HashMap<>();

    private HashMap<String, ArrayList<String>> loadMap() throws FileNotFoundException {
        HashMap<String, ArrayList<String>> map = new HashMap<>();
        Scanner scanner = new Scanner(new File(filePath + "/" + inputFile));
        while (scanner.hasNext()) {
            String[] splits = scanner.nextLine().trim().split("\\s+");
            int num = Integer.parseInt(splits[1]);
            String tableName = splits[0];
            boolean uniq = Boolean.parseBoolean(splits[3]);
            tableSize.put(tableName, num);
            uniqueNess.put(tableName, uniq);
            tupleSize.put(tableName, Integer.parseInt(splits[2]));
            //System.out.println(tableName);
            String[] attributes = scanner.nextLine().split("\\s+");
            ArrayList<String> arr = new ArrayList<>();
            for (int i = 0; i < num + 1 && i < attributes.length; i++) {
                arr.add(attributes[i]);
            }
            map.put(tableName, arr);
        }
        return map;
    }

    //Return the map which indicates the FK referencing relation. Key is the table name, Value is the referenced table and the corrspoondoing index.
    private HashMap<String, ArrayList<ComKey>> fkRelation(HashMap<String, ArrayList<String>> maps) {
        HashMap<String, ArrayList<ComKey>> result = new HashMap<>();
        for (Entry<String, ArrayList<String>> table : maps.entrySet()) {
            String tName = table.getKey();
            ArrayList<String> fks = new ArrayList<>();

            for (int i = 1; i < table.getValue().size(); i++) {
                String aName = table.getValue().get(i);
                if (aName.contains("-")) {
                    String[] temp = aName.split("-");
                    maps.get(tName).set(i, temp[0]);
                    String[] temps = temp[1].split(":");
                    // ArrayList<String> arr = new ArrayList<>();
                    ComKey comkey = new ComKey();
                    comkey.sourceTable = temps[0];
                    comkey.referencingTable = tName;
                    comkey.referenceposition = i;
                    if (!(tName.equals("socialgraph") && i == 2)) {
                        fks.add(temps[0]);
                        if (!result.containsKey(tName)) {
                            result.put(tName, new ArrayList<ComKey>());
                        }
                        if (!result.get(tName).contains(comkey)) {
                            result.get(tName).add(comkey);
                        }
                    } else {
                        result.get(tName).add(result.get(tName).get(0));
                    }
                }
            }

        }
        this.referencingTable = result;
        return result;
    }
    //Table[] originalDBtables;
    //  HashMap<String, Integer> tableMapping = new HashMap<>();

    private void loadTuple(Set<String> keySet, HashMap<String, ArrayList<String>> tableIDs) throws FileNotFoundException, IOException {

        ArrayList<Thread> liss = new ArrayList<>();
        //  HashMap<String, Table> db= new HashMap<>();
        this.originalDB.tables
                = new Table[keySet.size()];
        int count = 0;
        for (String table : keySet) {
            ParaReader pr = new ParaReader(table);
            pr.tables = this.originalDB.tables;
            originalDB.tableMapping.put(table, count);
            pr.tableNum = count;
            count++;

            pr.oldTableSize = this.oldTableSize;
            pr.ignoreFirst = this.ignoreFirst;
            pr.delim = this.delim;
            pr.filePath = this.filePath;
            pr.tableSize = tableSize.get(table);
            pr.tupleSize = this.tupleSize;
            Thread thr = new Thread(pr);

            liss.add(thr);
            thr.start();
        }
        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    //key [table1,table2], table 1 is the source table, table 2 is the referceing table.
    private void loadCounts(HashMap<String, ArrayList<ComKey>> fkRelation) {
        for (Entry<String, ArrayList<ComKey>> entry : fkRelation.entrySet()) {
            int count = 0;
            for (ComKey entry1 : entry.getValue()) {

                String tableName = entry.getKey();
                int colNum = entry1.referenceposition;
                if (tableName.equals("socialgraph") && count == 1) {
                    colNum = entry1.referenceposition + 1;
                    continue;
                }
                int tableNum = this.originalDB.tableMapping.get(tableName);

                String srcTable = entry1.sourceTable;
                int srcNum = this.originalDB.getTableNum(srcTable);

                IdFeatures idFeatures = new IdFeatures();
                idFeatures.idcounts = new int[this.originalDB.tables[srcNum].fks.length];

                for (int[] arr : this.originalDB.tables[tableNum].fks) {
                    int id = arr[colNum - 1];
                    idFeatures.idcounts[id] += 1;
                }
                this.originalCoDa.comKeyMapping.put(entry1, idFeatures);
                count++;
            }
        }
    }

    private void freCounts() {
        for (Entry<ComKey, IdFeatures> entry : this.originalCoDa.comKeyMapping.entrySet()) {
            HashMap<Integer, Integer> freCounts = new HashMap<>();
            for (int c : entry.getValue().idcounts) {
                if (!freCounts.containsKey(c)) {
                    freCounts.put(c, 1);
                } else {
                    freCounts.put(c, 1 + freCounts.get(c));
                }
            }
            this.originalCoDa.freCounts.put(entry.getKey(), freCounts);
        }
    }
    HashMap<String, Integer> scaleCounts = new HashMap<>();
    HashMap<String, Integer> scaleTableSize = new HashMap<>();

    private void scaleDistribution()
            throws FileNotFoundException {
        ArrayList<Thread> arr = new ArrayList<>();
        for (Entry<ComKey, HashMap<Integer, Integer>> entry : this.originalCoDa.freCounts.entrySet()) {
            ComKey key = entry.getKey();
            String sourceTable = key.sourceTable;
            String dependTable = key.referencingTable;
            OneKeyCombine okCombine = new OneKeyCombine(this.scaledCoda.freCounts, scaleCounts, key, entry.getValue());
            okCombine.s = 1.0*this.scaleTableSize.get(sourceTable) * 1.0 / this.oldTableSize.get(sourceTable);
            System.out.println(okCombine.s);
            if (entry.getKey().sourceTable.equals("socialgraph")) {
                okCombine.evenNum = true;
            }
            okCombine.dependAfter = this.scaleTableSize.get(dependTable);
            okCombine.sourceAfter = this.scaleTableSize.get(sourceTable);
            Thread thr = new Thread(okCombine);
            // thr.start();
            arr.add(thr);

        }
        for (Thread thr : arr) {
            thr.start();
            
        }
        for (Thread thr : arr) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public void run() throws FileNotFoundException, IOException {
        HashMap<String, ArrayList<String>> maps = loadMap();
        System.out.println("====================Map Loaded=============================");
        System.out.println(maps);

        //key is the foreign key name
        HashMap<String, ArrayList<ComKey>> fkRelation = fkRelation(maps);
        System.out.println(fkRelation);
        HashMap<String, ArrayList<String>> tableIDs = new HashMap<>();
        System.out.println("=========================loadTuples=======================");

        loadTuple(maps.keySet(), tableIDs);
     //   checkSocialGraph();
        // this.originalDB.tableMapping.get("socialgraph")
        this.scaleTableSize = loadSaleTable();
        System.gc();
        System.out.println("\n===============================Exteact Information====================");
        loadCounts(fkRelation);
        HashMap<String, ArrayList<ComKey>> mergedDegreeTitle = processMergeDegreeTitle(this.originalCoDa.comKeyMapping.keySet());
        System.out.println(mergedDegreeTitle);
        System.gc();
        System.out.println("======================Combine merged Degree=====================");
        processMergeDegree(mergedDegreeTitle);
        System.out.println("===========================Process correlation=================");

        loadCorrelation(mergedDegreeTitle);

        System.out.println("====================Process Key Distribution =====================");
        //  HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>>> reverseKeyDistribution = new HashMap<>();
        processBiDegree();

        this.originalCoDa.referenceVectors = null;

        System.out.println("======================Process FreCounts====================");
        freCounts();
        processMergeDistribution();
        HashMap<String, ArrayList<ComKey>> avaMaps = this.referencingTable;

        System.out.println("=======================Process AVA MAP======================");

        this.originalCoDa.mergedDegrees = null;
        this.originalCoDa.comKeyMapping = null;

        System.out.println("=====================Sort Correlation=======================");
        
        long start = System.currentTimeMillis();

        System.out.println("====================Scale Distribution======================= ");

        scaleDistribution();
        this.originalCoDa.freCounts = null; //memory cleaning

        long temp = System.currentTimeMillis();
        System.out.println("DS TIMING:" + (temp - start) / 1000);
        //  System.out.println("JDC ");
        this.originalDB.dropFKs();
        System.out.println("======================================JDC============================================");
        corrMap();

        this.scaledCoda.freCounts = null;
        long temp1 = System.currentTimeMillis();
        System.out.println("JDC Timing:" + (temp1 - temp) / 1000);

        System.out.println("=========================Some Computation=================================================");
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio = new HashMap<>();

        HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts = new HashMap<>();
        HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo = new HashMap<>();
        processAvaID(avaInfo, avaCounts);
        CorrelationVertex1 corrVertex = new CorrelationVertex1();
        corrVertex.scaleTableSize = this.scaleTableSize;
        corrVertex.oldTableSize = this.oldTableSize;

        corrVertex.referenceTable = this.referencingTable;
        System.err.println("Some Computation Time: " + (System.currentTimeMillis() - temp1) / 1000);
        System.out.println("======================Dismap Compuation============================");
        HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap = new HashMap<>();
        this.mergedDegreeTitle = mergedDegreeTitle;
        System.out.println("mergeDegree: " + this.mergedDegreeTitle);
        disMapComp(distanceMap, avaInfo);
        System.out.println(distanceMap.keySet() + "  " + distanceMap.keySet().size());
        corrVertex.distanceMap = distanceMap;
        temp = System.currentTimeMillis();
        System.err.println("Computation:" + (temp - temp1) / 1000);

        System.err.println("==============================RVC====================================================");
        corrVertex.originalCoDa = this.originalCoDa;
        corrVertex.mappedBestJointDegree = this.mappedBestJointDegree;
        corrVertex.uniqueNess = this.uniqueNess;
        this.scaledCoda.rvCorrelationDis
                = corrVertex.corrDist(this.scaledCoda.mergedDistribution, this.originalCoDa.rvCorrelationDis, mergedDegreeTitle,
                        downsizedMergedRatio, avaCounts);

        temp1 = System.currentTimeMillis();
        System.err.println("RVC Running time:" + (temp - temp1));

        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs = new HashMap<>(); //KEY IS THE DEGREE, VALUE ARE THOSE IDS WITH THE DEGREE

        ArrayList<String> keyTables = new ArrayList<>();
        ArrayList<String> refTables = new ArrayList<>();
        for (String entry : this.scaledCoda.rvCorrelationDis.keySet()) {
            refTables.add(entry);
        }

        ArrayList<String> joinTables = new ArrayList<>();
        for (String table : avaInfo.keySet()) {
            if (refTables.contains(table)) {
                keyTables.add(table);
                refTables.remove(table);
            } else {
                joinTables.add(table);
            }
        }
        System.out.println("====================Localized Join IDs==========================");
        localequatingMergedIDs(avaInfo, this.originalCoDa.reverseMergedDegrees, joinTables);

        //   this.originalCoDa.reverseMergedDegrees = null;
        System.out.println("==========================Assign IDs================================");

        HashMap<String, ArrayList<ArrayList<Integer>>> assignIDs
                = assignReferenceTable(this.scaledCoda.rvCorrelationDis, avaMaps, referencingIDs, avaInfo, this.originalCoDa.reverseReferenceVectors);
        temp = System.currentTimeMillis();

        System.err.println("Assign ID time:" + (temp - temp1));

        System.out.println("====================Matching ids=============================");
        HashMap<String, HashMap<Integer, Integer>> scaledBiMap = new HashMap<>();
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs = new HashMap<>();

        if (!this.originalCoDa.kvCorrelationDis.isEmpty()) {
            scaledBiMap = matchBiDegree(referencingIDs, avaInfo, this.originalCoDa.kvCorrelationDis, keyVectorIDs, keyTables);
        }
        temp1 = System.currentTimeMillis();
        this.originalCoDa.kvCorrelationDis = null; //empty the memory
        System.out.println("Matching ID time:" + (-temp + temp1));
        System.out.println("====================Localized IDs==========================");

        System.out.println("====================Localized Key IDs==========================");
        HashMap<String, HashMap<Integer, Integer>> localkeyMaps = localequatingIDs(keyVectorIDs, this.originalCoDa.reverseKeyVectors, keyTables);
        keyVectorIDs = null;
        //reverseKeyDistribution = null;
        this.originalCoDa.reverseKeyVectors = null;
        localKeyIDs(assignIDs, localkeyMaps, scaledBiMap, keyTables, "local");
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

        System.out.println("LocalEquating Time:" + (ended - temp1));
        System.out.println("LocalRunning time=" + (ended - start - 0) / 1000 + "s");
        PrintWriter time = new PrintWriter(new BufferedWriter(new FileWriter("time.txt", true)));
        time.println(this.s + "    " + (ended - start - 0) / 1000);
        //more code
        time.close();
     }

    private void processAvaID(
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo, HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts) {

        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> entry : this.scaledCoda.mergedDistribution.entrySet()) {

            int i = 0;
            ParaProcessAvaID ppad = new ParaProcessAvaID(i, entry, avaInfo, avaCounts);
            Thread thr = new Thread(ppad);
            liss.add(thr);

            thr.start();

            //   }
        }
        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private HashMap<String, ArrayList<ComKey>> processMergeDegreeTitle(Set<ComKey> keySet) {
        HashMap<String, ArrayList<ComKey>> result = new HashMap<>();
        for (ComKey key : keySet) {
            if (!result.containsKey(key.sourceTable)) {
                result.put(key.sourceTable, new ArrayList<ComKey>());
            }
            if (!result.get(key.sourceTable).contains(key)) {
                result.get(key.sourceTable).add(key);
            }
        }
        return result;
    }

    //mergedDegreeTitle was key=Source Table, Value are those referencing tables
    private void processMergeDegree(HashMap<String, ArrayList<ComKey>> mergedDegreeTitle
    ) {
        for (Entry<String, ArrayList<ComKey>> entry : mergedDegreeTitle.entrySet()) {
            ArrayList<ComKey> keys = new ArrayList<>();
            keys.addAll(entry.getValue());
            HashMap<ArrayList<Integer>, ArrayList<Integer>> mmap = new HashMap<>();
            String tableName = entry.getKey();
            int tableNum = this.originalDB.getTableNum(tableName);
            ArrayList<ArrayList<Integer>> allDegrees = new ArrayList<>(this.originalDB.tables[tableNum].fks.length);

            for (int pkid = 0; pkid < this.originalDB.tables[tableNum].fks.length; pkid++) {
                ArrayList<Integer> degrees = new ArrayList<>(keys.size());

                for (int o = 0; o < keys.size(); o++) {
                    ComKey group = keys.get(o);
                    degrees.add(this.originalCoDa.comKeyMapping.get(group).idcounts[pkid]);
                }
                if (!mmap.containsKey(degrees)) {
                    mmap.put(degrees, new ArrayList<Integer>());
                }
                mmap.get(degrees).add(pkid);
                allDegrees.add(degrees);
            }

            this.originalCoDa.reverseMergedDegrees.put(keys, mmap);
            this.originalCoDa.mergedDegrees.put(keys, allDegrees);
        }
    }

    private void processMergeDistribution() {
        for (Entry<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> entry : this.originalCoDa.mergedDegrees.entrySet()) {
            HashMap<ArrayList<Integer>, Integer> maps = new HashMap<>();
            for (int i = 0; i < entry.getValue().size(); i++) {
                if (!maps.containsKey(entry.getValue().get(i))) {
                    maps.put(entry.getValue().get(i), 1);
                } else {
                    maps.put(entry.getValue().get(i), 1 + maps.get(entry.getValue().get(i)));
                }
            }
            this.originalCoDa.mergedDistribution.put(entry.getKey(), maps);
        }
    }

    private void loadCorrelation(
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle
    ) throws FileNotFoundException {
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, ArrayList<ComKey>> entry : referencingTable.entrySet()) {

            ParaLoadCorr plc = new ParaLoadCorr();
            plc.originalDB = this.originalDB;
            plc.entry = entry;
            plc.mergedDegreeTitle = mergedDegreeTitle;
            plc.originalCoDa = this.originalCoDa;

            Thread thr = new Thread(plc);
            liss.add(thr);
            thr.start();

        }

        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    int indexcount = 0;

    private HashMap<String, HashMap<Integer, Integer>> localequatingIDs(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseDistribution,
            ArrayList<String> refTables) {
        HashMap<String, HashMap<Integer, Integer>> result = new HashMap<>();
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry : referencingIDs.entrySet()) {
            if (!entry.getKey().equals("socialgraph") && refTables.contains(entry.getKey())) {
                LocalRefTableGen lft = new LocalRefTableGen();
                lft.result = result;
                lft.reverseDistribution = reverseDistribution;
                lft.entry = entry;
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
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> entry : this.originalCoDa.mergedDistribution.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> arrs = new ArrayList<>();
            for (ComKey ck : entry.getKey()) {
                arrs.add(this.scaledCoda.freCounts.get(ck));
            }
            MergeVertex mergeVertex = new MergeVertex(this.scaledCoda.mergedDistribution, entry.getKey(), arrs, entry.getValue());
            settleCorrVetex(mergeVertex, entry.getKey().get(0).sourceTable);

            Thread thr = new Thread(mergeVertex);
            liss.add(thr);

        }

        for (Thread thr : liss) {
            thr.start();
        }

        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void disMapComp(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap,
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaCounts
    ) {
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<Integer>, AvaStat>> entry : avaCounts.entrySet()) {
            for (ComKey ck : mergedDegreeTitle.get(entry.getKey())) {
                ParaDisComp pdc = new ParaDisComp(distanceMap, entry, ck);
                //    Thread thr = new Thread(pdc);
                //   liss.add(thr);
                //     thr.start();
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
            System.out.println(entry.getKey());
            String refTable = "";
            for (String ref : assignIDs.keySet()) {
                if (ref.equals(curTable)) {
                    refTable = ref;
                }
            }
            //     int tableNum = originalDB.getTableNum(entry.getKey());
            if (keyTables.contains(entry.getKey())) {
                File file = new File(outFile + "/" +   entry.getKey() + ".txt");
                FileWriter writer = new FileWriter(file);
                BufferedWriter pw = new BufferedWriter(writer, 100000);
                int tableNum = this.originalDB.getTableNum(entry.getKey());
                int size = this.originalDB.tables[tableNum].tuplesize;
                for (Entry<Integer, Integer> entry2 : entry.getValue().entrySet()) {
                    pw.write("" + entry2.getKey());
                    int corId = scaledBiMap.get(entry.getKey()).get(entry2.getKey());
                   // if (!assignIDs.get(refTable).(corId)){
                   //     System.out.println("Somthing Wrong");
                   // }
                    for (int t : assignIDs.get(refTable).get(corId)) {
                        pw.write("|" + t);
                    }
                    int size1 = assignIDs.get(refTable).get(scaledBiMap.get(entry.getKey()).get(entry2.getKey())).size();
                    String rrid = "" + entry2.getValue();
                    int mappedPK = Integer.parseInt(rrid);
                    if (this.originalDB.tables[tableNum].nonKeys[mappedPK] != null) {
                        pw.write(  this.originalDB.tables[tableNum].nonKeys[mappedPK]);
                    }
                    pw.newLine();
                    count++;
                }

                pw.close();
                // tuples.remove(entry.getKey());

                System.out.println(count + " " + file + "   " + entry.getValue().size());

            }
        }
    }

    private void processBiDegree() {
        for (Entry<String, ArrayList<ArrayList<ArrayList<Integer>>>> rv : this.originalCoDa.referenceVectors.entrySet()) {
            String table = rv.getKey();
            for (Entry<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> joint_degree : this.originalCoDa.mergedDegrees.entrySet()) {
                if (table.equals(joint_degree.getKey().get(0).sourceTable)) {
                    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> vmap = new HashMap<>();
                    HashMap<ArrayList<ArrayList<Integer>>, Integer> map = new HashMap<>();
                    //ArrayList<ArrayList<ArrayList<Integer>>> kvDegrees = new ArrayList<>();

                    for (int i = 0; i < joint_degree.getValue().size(); i++) {
                        ArrayList<ArrayList<Integer>> kv = new ArrayList<>();
                        kv.add(joint_degree.getValue().get(i));
                        kv.addAll(rv.getValue().get(i));

                        if (!vmap.containsKey(kv)) {
                            vmap.put(kv, new ArrayList<Integer>());
                        }
                        vmap.get(kv).add(i);

                        //   kvDegrees.add(kv);
                        if (!map.containsKey(kv)) {
                            map.put(kv, 1);
                        } else {
                            map.put(kv, 1 + map.get(kv));
                        }
                    }
                    this.originalCoDa.kvCorrelationDis.put(table, map);
                    this.originalCoDa.reverseKeyVectors.put(table, vmap);
                    // this.originalCoDa.keyVectors.put(table, kvDegrees);

                }
            }
        }

    }

    private void sortCorrelation(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> tableCorrelationDistribution, HashMap<String, ArrayList<ComKey>> mergedDegreeTitle) {
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : tableCorrelationDistribution.entrySet()) {
            ParaSort ps = new ParaSort();
            ps.referencingTable = this.referencingTable;
            ps.mergedDegreeTitle = mergedDegreeTitle;
            ps.entry = entry;
//            ps.sortedCorrs = sortedCorrs;
            Thread thr = new Thread(ps);
            liss.add(thr);
            thr.start();
        }

        for (Thread thr : liss) {
            try {
                thr.join();
                //   HashMap<ArrayList<Integer>, Integer> correlated= produceCorr(entry.getValue(),arrs);
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
    ArrayList<Thread> idthread = new ArrayList<>();

    private HashMap<String, ArrayList<ArrayList<Integer>>> assignReferenceTable(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, ArrayList<ComKey>> avaMaps, HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseDistribution
    //        HashMap<String, HashMap<String, ArrayList<String>>> tuples
    ) {
        HashMap<String, ArrayList<ArrayList<Integer>>> assignIDs = new HashMap<>();
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : scaledCorrelationDistribution.entrySet()) {
            if (!avaInfo.containsKey(entry.getKey())) {
                ParaIdAssign para = new ParaIdAssign(entry, scaledCorrelationDistribution, avaMaps, referencingIDs, avaInfo);
                para.mergedDegreeTitle = this.mergedDegreeTitle;
                para.reverseDistribution = reverseDistribution;
                para.scaleTableSize = this.scaleTableSize;
                para.tableSize = this.tableSize;
                para.outFile = this.outFile;
                para.originalDB = this.originalDB;
                para.referencingTable = this.referencingTable;
                //   para.tuples = tuples;
                para.s = this.s;

                Thread thr = new Thread(para);
                idthread.add(thr);
                thr.start();
            } else {

                ParaKeyIdAssign para1 = new ParaKeyIdAssign(assignIDs, entry, scaledCorrelationDistribution, avaMaps, referencingIDs, avaInfo);
                para1.mergedDegreeTitle = this.mergedDegreeTitle;
                para1.reverseDistribution = reverseDistribution;
                para1.scaleTableSize = this.scaleTableSize;
                para1.tableSize = this.tableSize;
                Thread thr = new Thread(para1);
                liss.add(thr);
                thr.start();;

            }

        }
        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return assignIDs;
    }

    private HashMap<String, HashMap<Integer, Integer>> matchBiDegree(HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs1,
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> updatedDegree1,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> biDegreeCorrelation,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs, ArrayList<String> keyTables) {

        HashMap<String, HashMap<Integer, Integer>> ret = new HashMap<>();
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry : referencingIDs1.entrySet()) {
            if (keyTables.contains(entry.getKey())) {
                ParaMatch pm = new ParaMatch();
                pm.ret = ret;
                pm.s = this.scaleTableSize.get(entry.getKey()) * 1.0 / this.oldTableSize.get(entry.getKey());
                //   pm.referencingIDs = referencingIDs;
                pm.updatedDegree = updatedDegree1;
                pm.entry = entry;
                pm.keyVectorIDs = keyVectorIDs;
                pm.biDegreeCorrelation = biDegreeCorrelation;
                Thread thr = new Thread(pm);
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
        return ret;
    }

    private HashMap<String, HashMap<Integer, String>> localequatingMergedIDs(
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> updatedDegree,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseMergedDegree,
            ArrayList<String> joinTables) {
        //     HashMap<String, HashMap<String, ArrayList<String>>> tuples
        HashMap<String, HashMap<Integer, String>> result = new HashMap<>();
        ArrayList<Thread> liss = new ArrayList<>();
        for (Entry<String, HashMap<ArrayList<Integer>, AvaStat>> entry : updatedDegree.entrySet()) {
            if (joinTables.contains(entry.getKey())) {
                System.out.println("Join Table" + " " + entry.getKey());
                LocalMergeTableGen ljt = new LocalMergeTableGen();
                ljt.outFile = this.outFile;
                ljt.s = this.s;
                ljt.result = result;
                ljt.entry = entry;
                ljt.reverseMergedDegree = reverseMergedDegree;
                ljt.mergedDegreeTitle = this.mergedDegreeTitle;
                //  ljt.tuples = tuples;
                ljt.originalDB = this.originalDB;
                Thread thr = new Thread(ljt);
                liss.add(thr);
                thr.start();

            }
        }
        /*
         for (Thread thr : liss) {
         try {
         thr.join();
         } catch (InterruptedException ex) {
         Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
         }
         }*/
        return result;
    }

    private void checkSocialGraph() {
        if (this.originalDB.tableMapping.containsKey("socialgraph")) {
            int num = this.originalDB.getTableNum("socialgraph");
            for (int i = 0; i < this.originalDB.tables[num].fks.length; i = i + 2) {
                int first = this.originalDB.tables[num].fks[i][0];
                int second = this.originalDB.tables[num].fks[i][1];
                int first1 = this.originalDB.tables[num].fks[i + 1][0];
                int second1 = this.originalDB.tables[num].fks[i + 1][1];
                if (first != second1 || second != first1) {
                    System.out.println("socialgraph: " + i);
                    i = i - 1;

                }

            }

        }
    }

}
