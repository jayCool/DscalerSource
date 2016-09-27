/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

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
import javax.swing.text.html.parser.Entity;

/**
 *
 * @author workshop
 */
public class FSQ {

    /**
     * @param args the command line arguments
     */
    String inputFile = "config.txt";
    String statistics = "subconfig.txt";
    int scaledVertexSize = 0;
    double ratioOfFixedP = 0.0003;
    double s = 2.5;
    public String corrOriginal = "pre29Dec/keyDegree.txt";
    public String filePath = "test2";
    String delim=";";
    public static void main(String[] args) throws FileNotFoundException, IOException {
        //   filePath=args[0];
      //  FSQ fsq = new FSQ(args[0]);
       // fsq.s = Double.parseDouble(args[1]);

          FSQ fsq = new FSQ();
        fsq.run();
    }

    public FSQ(String string) {
        filePath = string;
    }

    FSQ() {
    }

    private void settleCorrVetex(MergeVertex corrVertex) {
        corrVertex.corrOriginal = this.corrOriginal;
        corrVertex.scaledVertexSize = this.scaledVertexSize;
        //     System.out.println(this.scaledVertexSize+"vertexSize");
        corrVertex.stime = s;
        corrVertex.rationP = this.ratioOfFixedP;
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

    private HashMap<String, ArrayList<String>> loadMap() throws FileNotFoundException {
        HashMap<String, ArrayList<String>> map = new HashMap<>();
        Scanner scanner = new Scanner(new File(filePath + "/" + inputFile));

        while (scanner.hasNext()) {
            String tableName = scanner.nextLine().trim();
            System.out.println(tableName);
            String[] attributes = scanner.nextLine().split("\\s+");
            ArrayList<String> arr = new ArrayList<>();
            for (String s : attributes) {
                arr.add(s);
            }
            map.put(tableName, arr);
        }
        return map;
    }

    private HashMap<String, ArrayList<ArrayList<String>>> fkRelation(HashMap<String, ArrayList<String>> maps) {
        HashMap<String, ArrayList<ArrayList<String>>> result = new HashMap<>();
        for (Entry<String, ArrayList<String>> table : maps.entrySet()) {
            String tName = table.getKey();
            ArrayList<String> fks = new ArrayList<>();

            for (int i = 0; i < table.getValue().size(); i++) {
                String aName = table.getValue().get(i);
                if (aName.contains("-")) {
                    String[] temp = aName.split("-");
                    maps.get(tName).set(i, temp[0]);
                    String[] temps = temp[1].split(":");
                    ArrayList<String> arr = new ArrayList<>();
                    arr.add("" + i);
                    arr.add(temps[0]);
                    fks.add(temps[0]);
                    if (!result.containsKey(tName)) {
                        result.put(tName, new ArrayList<ArrayList<String>>());
                    }
                    result.get(tName).add(arr);
                }
            }
            if (fks.size() > 0) {
                this.referencingTable.put(tName, fks);
            }
        }
        return result;
    }

    private HashMap<String, HashMap<String, ArrayList<String>>> loadTuple(Set<String> keySet, HashMap<String, ArrayList<String>> tableIDs) throws FileNotFoundException, IOException {
        ArrayList<String> arr = new ArrayList<>();

        HashMap<String, HashMap<String, ArrayList<String>>> result = new HashMap<>();
        for (String table : keySet) {
            System.out.println(table + ".txt");
            FileInputStream input1 = new FileInputStream(filePath + "/" + table + ".txt");
            BufferedReader scanner = new BufferedReader(new InputStreamReader(input1), 500000);
            //         BufferedReader scanner = new BufferedReader(new FileReader(new File(filePath + "/" + table + ".txt")));
            //   PrintWriter pw = new PrintWriter(filePath + "/nulladded" + table + ".txt");
            System.out.println(delim);
      if (this.ignoreFirst){ scanner.readLine().trim().split(delim);}
            //    int size = 

            //    System.out.println(scanner.readLine().trim().split(";").length);
            String input = scanner.readLine();
            String[] temp;
            int ssum = 0;
            HashMap<String, ArrayList<String>> maps = new HashMap<>();
            while (input != null) {
                //   input = input+" ";
                temp = input.trim().split(delim);
                if (temp.length >= temp.length) {
                    ssum = temp.length;
                    arr = new ArrayList<>();
                    for (int i = 1; i < temp.length; i++) {
                        String s = temp[i];
                       if (!s.trim().isEmpty()) {
                            arr.add(s.trim());
                        } else {
                            arr.add("null");
                        }
                    }
                    maps.put(temp[0], arr);
                }
                input = scanner.readLine();
            }
            System.out.println(ssum);
            result.put(table, maps);
            scanner.close();
         //   pw.close();
            //      System.gc();
        }
        return result;
    }

    //key [table1,table2], table 1 is the source table, table 2 is the referceing table.
    private HashMap<ArrayList<String>, HashMap<String, Integer>> loadCounts(HashMap<String, ArrayList<ArrayList<String>>> fkRelation, HashMap<String, HashMap<String, ArrayList<String>>> tuples) {
        HashMap<ArrayList<String>, HashMap<String, Integer>> result = new HashMap<>();
        for (Entry<String, ArrayList<ArrayList<String>>> entry : fkRelation.entrySet()) {
            for (ArrayList<String> entry1 : entry.getValue()) {
                if (entry1.size() > 0) {
                    ArrayList<String> tableKey = new ArrayList<>();
                    String sourceTable = entry1.get(1);
                    tableKey.add(sourceTable);
                    tableKey.add(entry.getKey());
                    //   if (entry.getKey().equals("socialgraph"))
                    HashMap<String, Integer> idCounts = new HashMap<>();
                    int colNum = Integer.parseInt(entry1.get(0));
                    for (ArrayList<String> arr : tuples.get(entry.getKey()).values()) {
                        if (arr.size() > 0) {
                            String id = arr.get(colNum - 1);
                            if (!idCounts.containsKey(id)) {
                                idCounts.put(id, 1);
                            } else {
                                idCounts.put(id, 1 + idCounts.get(id));
                            }
                        }
                    }
                //    if (entry.getKey().equals("socialgraph")) {
                    //       for (String s : idCounts.keySet()) {
                    //           idCounts.put(s, idCounts.get(s) / 2);
                    //       }
                    //    }

                    //       System.out.println(sourceTable);
                    for (String id : tuples.get(sourceTable).keySet()) {
                        //String id = arr.get(0);
                        if (!idCounts.containsKey(id)) {
                            idCounts.put(id, 0);
                        }
                    }

                    result.put(tableKey, idCounts);
                }
            }
        }
        return result;
    }

    private HashMap<ArrayList<String>, HashMap<Integer, Integer>> freCounts(HashMap<ArrayList<String>, HashMap<String, Integer>> idFeatures, HashMap<String, ArrayList<String>> idTables, HashMap<String, HashMap<String, ArrayList<String>>> tuples) {
        HashMap<ArrayList<String>, HashMap<Integer, Integer>> result = new HashMap<>();

        for (Entry<ArrayList<String>, HashMap<String, Integer>> entry : idFeatures.entrySet()) {
            HashMap<String, Integer> idCounts = entry.getValue();
            HashMap<Integer, Integer> freCounts = new HashMap<>();
            //  ArrayList<String> ids = idTables.get(entry.getKey().get(0));
            for (String id : tuples.get(entry.getKey().get(0)).keySet()) {
                int c = entry.getValue().get(id);
                if (!freCounts.containsKey(c)) {
                    freCounts.put(c, 1);
                } else {
                    freCounts.put(c, 1 + freCounts.get(c));
                }
            }
            result.put(entry.getKey(), freCounts);
        }
        return result;
    }
    HashMap<String, Integer> scaleCounts = new HashMap<>();

    private HashMap<ArrayList<String>, HashMap<Integer, Integer>> scaleDistribution(HashMap<ArrayList<String>, HashMap<Integer, Integer>> freCounts) throws FileNotFoundException {
        HashMap<ArrayList<String>, HashMap<Integer, Integer>> result = new HashMap<>();
        ArrayList<OneKeyCombine> arr = new ArrayList<>();
        for (Entry<ArrayList<String>, HashMap<Integer, Integer>> entry : freCounts.entrySet()) {
            ArrayList<String> key = entry.getKey();

            OneKeyCombine okCombine = new OneKeyCombine(result, scaleCounts, key, entry.getValue());
            okCombine.s = this.s;
            arr.add(okCombine);
            if (entry.getKey().get(1).equals("socialgraph")) {
                okCombine.evenNum = true;
            }
            (new Thread(okCombine)).start();

         //   System.out.println(entry.getKey());
      //            result.put(key, okCombine.runA(entry.getValue()));
            //     scaleCounts.put(entry.getKey().get(1), okCombine.dependAfter);
            //    scaleCounts.put(entry.getKey().get(0), okCombine.sourceAfter);
        }
        boolean sig = true;
        while (sig) {
            sig = false;
            for (OneKeyCombine k : arr) {
                if (!k.stoped) {
                    sig = true;
                    break;
                }
            }

        }
        return result;
    }
    String twoRef = "socialgraph";
    HashMap<String, ArrayList<String>> referencingTable = new HashMap<>();
    boolean ignoreFirst=true;
    public void run() throws FileNotFoundException, IOException {
        HashMap<String, ArrayList<String>> maps = loadMap();
        System.out.println(maps);
        HashMap<String, ArrayList<ArrayList<String>>> fkRelation = fkRelation(maps);
        System.out.println(fkRelation);
        HashMap<String, ArrayList<String>> tableIDs = new HashMap<>();
        System.out.println("loadTuples");
        HashMap<String, HashMap<String, ArrayList<String>>> tuples = loadTuple(maps.keySet(), tableIDs);
        System.out.println("Exteact Information");
        HashMap<ArrayList<String>, HashMap<String, Integer>> idFeatures = loadCounts(fkRelation, tuples);
        System.out.println(idFeatures.entrySet().iterator().next().getKey());
        //     System.out.println(idFeatures.entrySet().iterator().next().getValue());
        HashMap<String, ArrayList<String>> mergedDegreeTitle = processMergeDegreeTitle(idFeatures.keySet());
        HashMap<ArrayList<String>, HashMap< ArrayList<Integer>, ArrayList<String>>> reverseMergedDegree = new HashMap<>();
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>>> reverseDistribution = new HashMap<>();
        System.out.println("Combine merged Degree");
        HashMap<ArrayList<String>, HashMap<String, ArrayList<Integer>>> mergedDegree = processMergeDegree(mergedDegreeTitle, idFeatures, tableIDs, reverseMergedDegree, tuples);
        System.out.println("Process correlation");
        HashMap<ArrayList<String>, HashMap<String, ArrayList<ArrayList<Integer>>>> refTableKeyDegs = new HashMap<>();
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> tableCorrelationDistribution = loadCorrelation(mergedDegree, referencingTable, tuples, mergedDegreeTitle, reverseDistribution, refTableKeyDegs);
        System.out.println("Process Merge Distribution ");
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> biDegreeCorrelation = processBiDegree(refTableKeyDegs, mergedDegree);
        System.out.println("Process FreeCounts");
        HashMap<ArrayList<String>, HashMap<Integer, Integer>> freCounts = freCounts(idFeatures, tableIDs, tuples);
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution = processMergeDistribution(mergedDegree);
        HashMap<ArrayList<String>, ArrayList<ArrayList<String>>> avaMaps = new HashMap<>();
        System.out.println("Process AVA MAP");
        processAvaMap(avaMaps, tableCorrelationDistribution, mergedDegreeTitle);
        System.out.println("Sort Correlation");
        sortCorrelation(tableCorrelationDistribution, mergedDegreeTitle);
        System.out.println("Scale Distribution ");
        long start = System.currentTimeMillis();
        System.out.println("==================================================================================");
        HashMap<ArrayList<String>, HashMap<Integer, Integer>> downsizedCounts = scaleDistribution(freCounts);
        System.out.println("Merge Scale Distribution ");
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution = corrMap(mergedDistribution, downsizedCounts);
        //  mergeVertex.corrMap(mergedDistribution, downsizedCounts);
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio = processsRatio(mergedDistribution, downsizedMergedDistribution);
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree = new HashMap<>();
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts = new HashMap<>();
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> avaIDs = processAvaID(downsizedMergedDistribution, updatedDegree, avaCounts);
        System.out.println("Updated Degree");
        System.out.println("avaCounts: " + avaCounts);

        CorrelationVertex1 corrVertex = new CorrelationVertex1();
        corrVertex.stime = this.s;

        System.out.println("Dismap Compuation");
        HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap = new HashMap<>();
        disMapComp(distanceMap, avaCounts, downsizedMergedDistribution);
        corrVertex.distanceMap = distanceMap;

        System.out.println("Correlating Vertexes");
        corrVertex.sortedCorrs = this.sortedCorrs;
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution
                = corrVertex.corrDist(downsizedMergedDistribution, tableCorrelationDistribution, mergedDegreeTitle, downsizedMergedRatio, avaCounts);
       // scaledCorrelationDistribution = corrVertex.corrDist(downsizedMergedDistribution, tableCorrelationDistribution, mergedDegreeTitle, downsizedMergedRatio);

        //KEY IS THE DEGREE, VALUE ARE THOSE IDS WITH THE DEGREE
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs = new HashMap<>();
        System.out.println("Assign IDs");
        HashMap<ArrayList<String>, HashMap<Integer, ArrayList<Integer>>> assignIDs = assignReferenceTable(scaledCorrelationDistribution, avaIDs, avaMaps, referencingIDs);

        System.out.println("matching ids");
        HashMap<ArrayList<String>, HashMap<Integer, Integer>> scaledBiMap = new HashMap<>();
        if (!biDegreeCorrelation.isEmpty()) {
            scaledBiMap = matchBiDegree(referencingIDs, updatedDegree, biDegreeCorrelation);
        }

        System.out.println("Equating IDs");
        HashMap<ArrayList<String>, HashMap<Integer, String>> referMaps = equatingMergedIDs(updatedDegree, reverseMergedDegree);
        long omited = System.currentTimeMillis();
        outputIDs2(updatedDegree, referMaps, tuples);
//omited = System.currentTimeMillis()-omited;
        referMaps = equatingIDs(referencingIDs, reverseDistribution);
        long ended = System.currentTimeMillis();
        System.out.println("Running time="+ (ended - start - 0)/1000+"s");
        outputIDs(assignIDs, referMaps, tuples, scaledBiMap);
      //  referMaps = new HashMap<>();

    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> processAvaID(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts) {
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> result = new HashMap<>();
        ArrayList<ParaProcessAvaID> liss = new ArrayList<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : downsizedMergedDistribution.entrySet()) {
            HashMap<ArrayList<Integer>, ArrayList<Integer>> aar = new HashMap<>();

            for (int i = 1; i < entry.getKey().size(); i++) {
                ParaProcessAvaID ppad = new ParaProcessAvaID(result, aar, updatedDegree, i, entry, avaCounts);
                liss.add(ppad);
                (new Thread(ppad)).start();;
                /*       int startID = 1;
                 ArrayList<String> keys = new ArrayList<>();
                 keys.add(entry.getKey().get(0));
                 keys.add(entry.getKey().get(i));
                 HashMap<ArrayList<Integer>, HashMap<Integer, Integer>> ava = new HashMap<>();
                 //ParaProcessAvaID ppa = new ParaProcessAvaID(result,entry,i,updatedDegree);
                 for (Entry<ArrayList<Integer>, Integer> entry2 : entry.getValue().entrySet()) {
                 HashMap<Integer, Integer> queue = new HashMap<>();
                 int repetNum = entry2.getKey().get(i - 1);
                 if (!aar.containsKey(entry2.getKey())) {
                 aar.put(entry2.getKey(), new ArrayList<Integer>());
                 }
                 for (int j = 1; j <= entry2.getValue(); j++) {
                 if (i == 1) {
                 aar.get(entry2.getKey()).add(j + startID);
                 }
                 queue.put(j + startID, repetNum);
                 }
                 startID += entry2.getValue();
                 ava.put(entry2.getKey(), queue);
                 }
                
                 if (i == 1) {
                 updatedDegree.put(entry.getKey(), aar);
                 }
                 result.put(keys, ava);*/
            }
        }
        boolean sig = true;
        while (sig) {
            sig = false;
            for (ParaProcessAvaID k : liss) {
                if (!k.stop) {
                    sig = true;
                    break;
                }
            }

        }
        System.out.println("avaCounts: " + avaCounts.size());
        return result;
    }

    private HashMap<String, ArrayList<String>> processMergeDegreeTitle(Set<ArrayList<String>> keySet) {
        HashMap<String, ArrayList<String>> result = new HashMap<>();
        for (ArrayList<String> arr : keySet) {
            if (!result.containsKey(arr.get(0))) {
                result.put(arr.get(0), new ArrayList<String>());
            }
            result.get(arr.get(0)).add(arr.get(1));
        }
        return result;
    }

    private HashMap<ArrayList<String>, HashMap<String, ArrayList<Integer>>> processMergeDegree(HashMap<String, ArrayList<String>> mergedDegreeTitle, HashMap<ArrayList<String>, HashMap<String, Integer>> idFeatures, HashMap<String, ArrayList<String>> tableIDs, HashMap<ArrayList<String>, HashMap< ArrayList<Integer>, ArrayList<String>>> reverseMergedDegree, HashMap<String, HashMap<String, ArrayList<String>>> tuples) {
        HashMap<ArrayList<String>, HashMap<String, ArrayList<Integer>>> result = new HashMap<>();
        for (Entry<String, ArrayList<String>> entry : mergedDegreeTitle.entrySet()) {
            HashMap<String, ArrayList<Integer>> degreeMaps = new HashMap<>();
            ArrayList<String> keys = new ArrayList<>();
            keys.add(entry.getKey());
            keys.addAll(entry.getValue());
            //  reverseMergedDegree.put(keys, new HashMap<>());
            HashMap< ArrayList<Integer>, ArrayList<String>> mmap = new HashMap<>();
            ArrayList<ArrayList<String>> idDegree = new ArrayList<>();
            ArrayList<ArrayList<String>> idTitles = new ArrayList<>();
            for (String s : entry.getValue()) {
                ArrayList<String> group = new ArrayList<>();
                group.add(entry.getKey());
                group.add(s);
                idTitles.add(group);
            }
            ArrayList<Integer> sum = new ArrayList<>();
            for (int k = 0; k < idTitles.size(); k++) {
                sum.add(0);
            }
            for (String id : tuples.get(entry.getKey()).keySet()) {
                ArrayList<Integer> degrees = new ArrayList<>();
                for (int o = 0; o < idTitles.size(); o++) {
                    ArrayList<String> group = idTitles.get(o);
                    if (idFeatures.get(group).get(id) != null) {
                        degrees.add(idFeatures.get(group).get(id));
                        sum.set(o, sum.get(o) + idFeatures.get(group).get(id));
                    } else {
                        degrees.add(0);
                        idFeatures.get(group).put(id, 0);

                    }
                }
                if (!mmap.containsKey(degrees)) {
                    mmap.put(degrees, new ArrayList<String>());
                }
                mmap.get(degrees).add(id);
                degreeMaps.put(id, degrees);
            }
            System.out.println(sum);
            //       System.exit(0);
            reverseMergedDegree.put(keys, mmap);
            result.put(keys, degreeMaps);
        }

        return result;
    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> processMergeDistribution(HashMap<ArrayList<String>, HashMap<String, ArrayList<Integer>>> mergedDegree) {
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> result = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<String, ArrayList<Integer>>> entry : mergedDegree.entrySet()) {
            HashMap<ArrayList<Integer>, Integer> maps = new HashMap<>();
            for (Entry<String, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
                if (!maps.containsKey(entry2.getValue())) {
                    maps.put(entry2.getValue(), 1);
                } else {
                    maps.put(entry2.getValue(), 1 + maps.get(entry2.getValue()));
                }
            }
            result.put(entry.getKey(), maps);
        }
        return result;
    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> loadCorrelation(HashMap<ArrayList<String>, HashMap<String, ArrayList<Integer>>> mergedDegree, HashMap<String, ArrayList<String>> referencingTable, HashMap<String, HashMap<String, ArrayList<String>>> tuples, HashMap<String, ArrayList<String>> mergedDegreeTitle, HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>>> reverseDistribution, HashMap<ArrayList<String>, HashMap<String, ArrayList<ArrayList<Integer>>>> refTableKeyDegs) throws FileNotFoundException {
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result = new HashMap<>();
        HashMap<String, HashSet<String>> missingIDs = new HashMap<>();
        for (Entry<String, ArrayList<String>> entry : referencingTable.entrySet()) {
            HashMap<String, ArrayList<ArrayList<Integer>>> mapRefDegree = new HashMap<>();

            HashSet<String> sets = new HashSet<>();
            HashMap<ArrayList<ArrayList<Integer>>, Integer> degreeCount = new HashMap<>();
            ArrayList<String> keys = new ArrayList<>();
            keys.add(entry.getKey());
            keys.addAll(entry.getValue());
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>> mmap = new HashMap<>();
            HashMap<Integer, ArrayList<String>> combinedKeys = new HashMap<>();
            for (int i = 1; i < keys.size(); i++) {
                ArrayList<String> targetTitle = new ArrayList<>();
                targetTitle.add(keys.get(i));
                targetTitle.addAll(mergedDegreeTitle.get(keys.get(i)));
                combinedKeys.put(i, targetTitle);
            }
            for (Entry<String, ArrayList<String>> entrys : tuples.get(entry.getKey()).entrySet()) {
                ArrayList<String> tupleValue = entrys.getValue();

                ArrayList<ArrayList<Integer>> degree = new ArrayList<>();
                for (int i = 1; i <= combinedKeys.size(); i++) {
                    degree.add(mergedDegree.get(combinedKeys.get(i)).get(tupleValue.get(i - 1)));
                    if (mergedDegree.get(combinedKeys.get(i)).get(tupleValue.get(i - 1))==null){
                    System.out.println(entrys.getKey()+" "+tupleValue.get(i - 1));
                    }
                    if (degree.get(i - 1) == null) {
                        if (!missingIDs.containsKey(combinedKeys.get(i).get(0))) {
                            missingIDs.put(combinedKeys.get(i).get(0), new HashSet<String>());
                        }
                        missingIDs.get(combinedKeys.get(i).get(0)).add(tupleValue.get(i - 1));
                    }
                }
                mapRefDegree.put(entrys.getKey(), degree);
                if (!degreeCount.containsKey(degree)) {
                    degreeCount.put(degree, 1);
                } else {
                    degreeCount.put(degree, 1 + degreeCount.get(degree));
                }
                if (!mmap.containsKey(degree)) {
                    mmap.put(degree, new ArrayList<String>());
                }
                mmap.get(degree).add(entrys.getKey());
            }
            refTableKeyDegs.put(keys, mapRefDegree);
            reverseDistribution.put(keys, mmap);
            result.put(keys, degreeCount);
        }

        //  for (Entry<String,HashSet<String>> entry:missingIDs.entrySet()){
        //    PrintWriter pw = new PrintWriter(entry.getKey());
        //     for (String s:entry.getValue()) pw.println(s);
        //     pw.close();
        //  }
        return result;
    }
//18659995028

    private HashMap<ArrayList<String>, HashMap<Integer, ArrayList<Integer>>> assignReferenceTable(HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> avaIDs, HashMap<ArrayList<String>, ArrayList<ArrayList<String>>> avaMaps, HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs) {
        HashMap<ArrayList<String>, HashMap<Integer, ArrayList<Integer>>> assignIDs = new HashMap<>();

        //key is the referencing table 
        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : scaledCorrelationDistribution.entrySet()) {

            HashMap<Integer, ArrayList<Integer>> ids = new HashMap<>();
            int id = 1;
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap = new HashMap<>();
            for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 : entry.getValue().entrySet()) {
                Queue<ArrayList<Integer>> queue = new LinkedList<>();
                for (int i = 0; i < entry2.getValue(); i++) {
                    queue.offer(new ArrayList<Integer>());
                }
                if (!mmap.containsKey(entry2.getKey())) {
                    mmap.put(entry2.getKey(), new ArrayList<Integer>());
                }
                for (int k = 0; k < entry2.getKey().size() && entry2.getValue() != 0; k++) {
                    ArrayList<Integer> degrees = entry2.getKey().get(k);
                    ArrayList<String> attr = avaMaps.get(entry.getKey()).get(k);
                    int count = entry2.getValue();
                    int n = avaIDs.get(attr).get(degrees).keySet().size();
                    int quo = count / n;
                    int remain = count % n;
                    HashMap<Integer, Integer> idFreq = avaIDs.get(attr).get(degrees);
                    if (entry.getKey().get(0).equals("partsupp") && k == 0) {
                //    System.out.println(attr);
                        //   System.out.println(idFreq);
                        //     System.exit(0);
                    }
                    Sort sort = new Sort();
                    List<Entry<Integer, Integer>> sorted = sort.sortOnValueInteger(idFreq);

                    HashMap<Integer, Integer> curIDs = new HashMap<>();
                    for (int i = 0; i < sorted.size(); i++) {
                        int key = sorted.get(i).getKey();
                        if (i < remain) {
                            curIDs.put(key, quo + 1);
                            avaIDs.get(attr).get(degrees).put(key, sorted.get(i).getValue() - quo - 1);
                        } else {
                            curIDs.put(key, quo);
                            avaIDs.get(attr).get(degrees).put(key, sorted.get(i).getValue() - quo);
                        }
                        if (avaIDs.get(attr).get(degrees).get(key) == 0) {
                            avaIDs.get(attr).get(degrees).remove(key);
                        }
                    }
                    for (Entry<Integer, Integer> idEntry : curIDs.entrySet()) {
                        for (int i = 0; i < idEntry.getValue(); i++) {
                            ArrayList<Integer> arr = new ArrayList<>();
                            ArrayList<Integer> pre = queue.poll();
                            for (int j = 0; j < pre.size(); j++) {
                                arr.add(pre.get(j));
                            }
                            arr.add(idEntry.getKey());
                            queue.offer(arr);
                        }
                    }
                    if (k < entry2.getKey().size()) {
                        HashMap<ArrayList<Integer>, Integer> maps = new HashMap<>();
                        int size = queue.size();
                        while (!queue.isEmpty()) {
                            if (!maps.containsKey(queue.peek())) {
                                maps.put(queue.peek(), 0);
                            }
                            maps.put(queue.peek(), maps.get(queue.poll()) + 1);
                        }

                        Sort so = new Sort();
                        List<Entry<ArrayList<Integer>, Integer>> sorte = so.sortOnValueIntegerDesc(maps);

                        while (size > 0) {
                            for (int i = 0; i < sorte.size() && size > 0; i++) {
                                queue.offer(sorte.get(i).getKey());
                                size--;
                            }
                        }
                    }
                }

                while (!queue.isEmpty()) {
                    mmap.get(entry2.getKey()).add(id);
                    ids.put(id, queue.poll());
                    id++;
                }

            }

            assignIDs.put(entry.getKey(), ids);
            referencingIDs.put(entry.getKey(), mmap);
        }
        return assignIDs;
    }

    private void outputIDs(HashMap<ArrayList<String>, HashMap<Integer, ArrayList<Integer>>> assignIDs, HashMap<ArrayList<String>, HashMap<Integer, String>> referMaps, HashMap<String, HashMap<String, ArrayList<String>>> tuples, HashMap<ArrayList<String>, HashMap<Integer, Integer>> scaledBiMap) throws FileNotFoundException, IOException {
        for (Entry<ArrayList<String>, HashMap<Integer, ArrayList<Integer>>> entry : assignIDs.entrySet()) {
            if (!entry.getKey().get(0).equals("socialgraph1")) {
                File file = new File(filePath + "s_" + this.s + entry.getKey().get(0) + ".txt");
                FileWriter writer = new FileWriter(file);
                BufferedWriter pw = new BufferedWriter(writer, 100000);
                System.out.println(file);
                int size = tuples.get(entry.getKey().get(0)).values().iterator().next().size();
                //  int size1 = entry.getValue().values().iterator().next().size();
                for (Entry<Integer, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
                    int keyid = entry2.getKey();
                    int transid = keyid;
                    if (scaledBiMap.containsKey(entry.getKey())) {
                        if (scaledBiMap.get(entry.getKey()).containsKey(keyid)) {
                            transid = scaledBiMap.get(entry.getKey()).get(keyid);
                        }
                    }
                    int size1 = entry2.getValue().size();
                    //   if (entry.getKey().get(0).equals("partsupp")) System.out.println("size1"+size1+entry2.getValue());
                    pw.write("" + transid);
                    for (int i : entry2.getValue()) {
                        pw.write(";" + i);
                    }
                    if (size > size1) {
                        String rrid = "";
                        if (referMaps.get(entry.getKey()).containsKey(entry2.getKey())) {
                            rrid = referMaps.get(entry.getKey()).get(entry2.getKey());
                        } else {
                            //       rrid = tuples.get(entry.getKey().get(0)).keySet().
                        }
                        for (int i = size1; i < size && rrid != null; i++) {
                            pw.write(";" + tuples.get(entry.getKey().get(0)).get(rrid).get(i).trim());
                        }
                    }
                    pw.newLine();
                }
                //  writer.close();
                pw.close();

            } else {
                ArrayList<ArrayList<Integer>> values = new ArrayList<>();
                values.addAll(entry.getValue().values());
                HashSet<ArrayList<Integer>> repeated = new HashSet<>();
                for (ArrayList<Integer> arr : values) {
                    ArrayList<Integer> reverse = new ArrayList<>();
                    for (int i = arr.size() - 1; i >= 0; i--) {
                        reverse.add(arr.get(i));
                    }
                    if (values.contains(reverse)) {
                        repeated.add(arr);
                        repeated.add(reverse);
                    }
                }
                ArrayList<ArrayList<Integer>> nonrepeated = new ArrayList<>();
                ConcurrentHashMap<Integer, ArrayList<Integer>> maps = new ConcurrentHashMap<>();

                for (ArrayList<Integer> arr : values) {
                    nonrepeated.add(arr);
                    if (!maps.containsKey(arr.get(0))) {
                        maps.put(arr.get(0), new ArrayList<Integer>());
                    }
                    maps.get(arr.get(0)).add(arr.get(1));
                }
                while (nonrepeated.size() <= values.size()) {
                    for (ArrayList<Integer> arr : nonrepeated) {
//                        int 
                    }
                }

            }
        }
    }

    int indexcount = 0;

    private HashMap<ArrayList<String>, HashMap<Integer, String>> equatingIDs(HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs, HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>>> reverseDistribution) {
        HashMap<ArrayList<String>, HashMap<Integer, String>> result = new HashMap<>();
        ArrayList<ArrayList<ArrayList<Integer>>> calDegs = new ArrayList<>();
        ArrayList<ArrayList<Integer>> calDeg = new ArrayList<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry : referencingIDs.entrySet()) {
            if (!entry.getKey().get(0).equals("socialgraph")) {
                
            //count the frequency of the appeared rrid    
            HashMap<String, Integer> rridFre = new HashMap<>();
       
                System.out.println(entry.getKey());
                HashMap<Integer, String> mmap = new HashMap<>();
                HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> norm1Map = this.l1NormMap(reverseDistribution.get(entry.getKey()).keySet());
                for (Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
                    int leftOver = entry2.getValue().size();
                    while (leftOver > 0 && !reverseDistribution.get(entry.getKey()).keySet().isEmpty()) {
                        calDeg = normMapCandidate(norm1Map, entry2.getKey());
                        while (reverseDistribution.get(entry.getKey()).get(calDeg).size() == 0) {
                            norm1Map.get(indexcount).remove(calDeg);
                            calDeg = normMapCandidate(norm1Map, entry2.getKey());
                        }
                        int size = reverseDistribution.get(entry.getKey()).get(calDeg).size();
                        while (0 < size && leftOver > 0) {
                            int id = entry2.getValue().get(leftOver - 1);
                            int tsize = (int) (Math.random() * (size - 1));
                            
                         //   String rrid = reverseDistribution.get(entry.getKey()).get(calDeg).remove(tsize);
                         //  size--;
                           
                            String rrid = "";
                        if (s < 1) {
                            rrid = reverseDistribution.get(entry.getKey()).get(calDeg).remove(tsize);
                            size--;
                        } else {
                            rrid = reverseDistribution.get(entry.getKey()).get(calDeg).get(tsize);
                            if (!rridFre.containsKey(rrid)) {
                                rridFre.put(rrid, 1);
                            } else {
                                rridFre.put(rrid, 1 + rridFre.get(rrid));
                            }
                            if (rridFre.get(rrid)>this.s){
                            reverseDistribution.get(entry.getKey()).get(calDeg).remove(tsize);
                             size--;
                            }
                        }
                           if (size == 0) {
                                norm1Map.get(indexcount).remove(calDeg);
                            }
                            leftOver--;
                            mmap.put(id, rrid);
                        }
                    }
                    if (reverseDistribution.get(entry.getKey()).keySet().isEmpty()) {
                        System.out.println("error" + leftOver);
                    }
                }
                result.put(entry.getKey(), mmap);
            }
        }
        return result;
    }

    private HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> l1NormMap(Set<ArrayList<ArrayList<Integer>>> keySet) {

        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> map = new HashMap<>();
        int sumde = 0;

//System.out.println(keySet.size());
        for (ArrayList<ArrayList<Integer>> temp : keySet) {
            for (ArrayList<Integer> arr : temp) {
                int sumt = 0;
                for (int i : arr) {
                    sumt += i;
                }
                if (!map.containsKey(Math.abs(sumt - sumde))) {
                    map.put(Math.abs(sumt - sumde), new ArrayList<ArrayList<ArrayList<Integer>>>());
                }
                map.get(Math.abs(sumt - sumde)).add(temp);
            }
        }

        //  System.out.println(map.size());
        return map;

    }

    private ArrayList<ArrayList<Integer>> l1Norm2Sets(ArrayList<Integer> pair, Set<ArrayList<Integer>> keySet) {
        int maxABS = 0;

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();
        int sumde = 0;
        for (int i : pair) {
            sumde += i;
        }

//System.out.println(keySet.size());
        for (ArrayList<Integer> arr : keySet) {
            int sumt = 0;
            for (int i : arr) {
                sumt += i;
            }
            maxABS = Math.max(maxABS, Math.abs(sumt - sumde));

            if (!map.containsKey(Math.abs(sumt - sumde))) {
                map.put(Math.abs(sumt - sumde), new ArrayList<ArrayList<Integer>>());
            }
            map.get(Math.abs(sumt - sumde)).add(arr);
        }

        //  System.out.println(map.size());
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i <= 2 * maxABS; i++) {
            if (map.containsKey(i)) {
                result.addAll(map.get(i));
            }
        }
        return result;

    }

    private HashMap<ArrayList<String>, HashMap<Integer, String>> equatingMergedIDs(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<String>>> reverseMergedDegree) {
        HashMap<ArrayList<String>, HashMap<Integer, String>> result = new HashMap<>();
        ArrayList<ArrayList<Integer>> calDegs = new ArrayList<>();
        ArrayList<Integer> calDeg = new ArrayList<>();

        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> entry : updatedDegree.entrySet()) {
            HashMap<Integer, String> mmap = new HashMap<>();

            //count the frequency of the appeared rrid    
            HashMap<String, Integer> rridFre = new HashMap<>();
            //  System.out.println(entry.getKey());
            for (Entry<ArrayList<Integer>, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
                int leftOver = entry2.getValue().size();
                calDegs = l1Norm2Sets(entry2.getKey(), reverseMergedDegree.get(entry.getKey()).keySet());
                int st = 0;
                while (leftOver > 0) {
                    calDeg = calDegs.get(st);
                    while (reverseMergedDegree.get(entry.getKey()).get(calDeg).isEmpty()) {
                        reverseMergedDegree.get(entry.getKey()).remove(calDeg);
                        st++;
                        calDeg = calDegs.get(st);
                    }
                    int size = reverseMergedDegree.get(entry.getKey()).get(calDeg).size();
                    while (0 < size && leftOver > 0) {
                        int id = entry2.getValue().get(leftOver - 1);
                        int tsize = (int) (Math.random() * (size - 1));
                        String rrid = "";
                        if (s < 1) {
                            rrid = reverseMergedDegree.get(entry.getKey()).get(calDeg).remove(tsize);
                            size--;
                        } else {
                            rrid = reverseMergedDegree.get(entry.getKey()).get(calDeg).get(tsize);
                            if (!rridFre.containsKey(rrid)) {
                                rridFre.put(rrid, 1);
                            } else {
                                rridFre.put(rrid, 1 + rridFre.get(rrid));
                            }
                            if (rridFre.get(rrid)>this.s){
                            reverseMergedDegree.get(entry.getKey()).get(calDeg).remove(tsize);
                             size--;
                            }
                        }

                        if (size == 0) {
                            reverseMergedDegree.get(entry.getKey()).remove(calDeg);
                            st++;
                        }
                        leftOver--;
                        mmap.put(id, rrid);
                    }

                }
            }
            result.put(entry.getKey(), mmap);
        }
        return result;
    }

    private void outputIDs2(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree, HashMap<ArrayList<String>, HashMap<Integer, String>> referMaps, HashMap<String, HashMap<String, ArrayList<String>>> tuples) throws FileNotFoundException, IOException {
        for (Entry<ArrayList<String>, HashMap<Integer, String>> entry : referMaps.entrySet()) {
            if (!entry.getKey().get(0).equals("socialgraph1")) {
                File file = new File(filePath + "/s_" + this.s + entry.getKey().get(0) + ".txt");
                FileWriter writer = new FileWriter(file);
                BufferedWriter pw = new BufferedWriter(writer, 100000);

                //         PrintWriter pw = new PrintWriter(filePath + "/s_" + entry.getKey().get(0) + ".txt");
                for (Entry<Integer, String> entry2 : entry.getValue().entrySet()) {
                    pw.write("" + entry2.getKey());
                    String rrid = entry2.getValue();
                    //  System.out.println(rrid);
                    //  System.out.println(entry.getKey());
                    //  System.out.println();
                    for (String s : tuples.get(entry.getKey().get(0)).get(rrid)) {
                        pw.write(";" + s.trim());
                    }
                    pw.newLine();
                }
                pw.close();

            }
        }
    }

    String corrFile = "corrFile.txt";
    String mergeFile = "mergeFile.txt";
    String reverseMerge = "reverseMerge.txt";
    String reverseCorr = "reverseCorr.txt";
    String scaledCorr = "scaledCorrFile.txt";

    private ArrayList<ArrayList<Integer>> normMapCandidate(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> norm1Map, ArrayList<ArrayList<Integer>> pair) {
        int sumde = 0;
        for (ArrayList<Integer> arr : pair) {
            for (int i : arr) {
                sumde += i;
            }
        }

        int max = Integer.MAX_VALUE;
        for (int i = 0; i < max; i++) {
            if (norm1Map.containsKey(i + sumde) && !norm1Map.get(i + sumde).isEmpty()) {
                indexcount = i + sumde;
                return norm1Map.get(indexcount).get(0);
            }
            if (norm1Map.containsKey(-i + sumde) && !norm1Map.get(-i + sumde).isEmpty()) {
                indexcount = -i + sumde;
                return norm1Map.get(indexcount).get(0);
            }

        }
        System.out.println("error");
        return null;
    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> processsRatio(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution) {
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> result = new HashMap<>();
        ArrayList<ParallelProcessRatio> liss = new ArrayList<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : downsizedMergedDistribution.entrySet()) {
            ParallelProcessRatio ppr = new ParallelProcessRatio(result, entry, mergedDistribution);
            (new Thread(ppr)).start();

        }
        boolean sig = true;
        while (sig) {
            sig = false;
            for (ParallelProcessRatio k : liss) {
                if (!k.stop) {
                    sig = true;
                    break;
                }
            }

        }
        return result;
    }

    private HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> processBiDegree(HashMap<ArrayList<String>, HashMap<String, ArrayList<ArrayList<Integer>>>> refTableKeyDegs, HashMap<ArrayList<String>, HashMap<String, ArrayList<Integer>>> mergedDegree) {
        HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<String, ArrayList<ArrayList<Integer>>>> entry : refTableKeyDegs.entrySet()) {
            String table = entry.getKey().get(0);

            HashMap<ArrayList<ArrayList<Integer>>, Integer> map = new HashMap<>();
            for (Entry<ArrayList<String>, HashMap<String, ArrayList<Integer>>> entry2 : mergedDegree.entrySet()) {
                if (table.equals(entry2.getKey().get(0))) {
                    for (Entry<String, ArrayList<Integer>> entry3 : entry2.getValue().entrySet()) {
                        ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
                        arr.add(entry3.getValue());
                        arr.addAll(entry.getValue().get(entry3.getKey()));
                        if (!map.containsKey(arr)) {
                            map.put(arr, 1);
                        } else {
                            map.put(arr, 1 + map.get(arr));
                        }
                    }
                    result.put(table, map);
                }
            }
        }
        return result;
    }

    private HashMap<ArrayList<String>, HashMap<Integer, Integer>> matchBiDegree(HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs1, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree1, HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> biDegreeCorrelation) {
        HashMap<ArrayList<String>, HashMap<Integer, Integer>> ret = new HashMap<>();
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs = copyRefer(referencingIDs1);
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree = copyUpdate(updatedDegree1);
        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry : referencingIDs.entrySet()) {
            String table = entry.getKey().get(0);
            ArrayList<String> merTitle = new ArrayList<>();
            for (ArrayList<String> ar : updatedDegree.keySet()) {
                if (ar.get(0).equals(table)) {
                    merTitle = ar;
                    break;
                }
            }
            HashMap<Integer, Integer> mapid = new HashMap<>();
            if (!merTitle.isEmpty()) {
                HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> referenceMap = l1NormMap(entry.getValue().keySet());
                HashMap<Integer, ArrayList<ArrayList<Integer>>> degMap = this.indegreeMap(updatedDegree.get(merTitle).keySet());
                for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 : biDegreeCorrelation.get(table).entrySet()) {
                    if (referenceMap.size() > 0 && degMap.size() > 0) {
                        int value = (int) (entry2.getValue() * this.s + 1);
                        while (value > 0 && referenceMap.size() > 0 && degMap.size() > 0) {
                            ArrayList<ArrayList<Integer>> refer = new ArrayList<>();
                            for (int i = 0; i < entry2.getKey().size() - 1; i++) {
                                refer.add(entry2.getKey().get(0));
                            }
                            ArrayList<ArrayList<Integer>> calDeg = normMapCandidate(referenceMap, refer);
                            ArrayList<Integer> calmerge = mergeMapCandidate(degMap, refer.get(refer.size() - 1));
                            ArrayList<Integer> refIDs = entry.getValue().get(calDeg);
                            ArrayList<Integer> mergIDs = updatedDegree.get(merTitle).get(calmerge);
                            int it = Math.min(refIDs.size(), mergIDs.size());
                            it = Math.min(it, value);
                            for (int i = 0; i < it; i++) {
                                value--;
                                int refid = refIDs.remove(0);
                                int mergid = mergIDs.remove(0);
                                mapid.put(refid, mergid);
                            }
                            if (refIDs.size() == 0) {
                                referenceMap.get(indexcount).remove(calDeg);
                                if (referenceMap.get(indexcount).size() == 0) {
                                    referenceMap.remove(indexcount);
                                }
                            }

                            if (mergIDs.size() == 0) {
                                degMap.get(mergeindexcount).remove(calmerge);
                                if (degMap.get(mergeindexcount).size() == 0) {
                                    degMap.remove(mergeindexcount);
                                }
                            }

                        }
//                    ArrayList<Integer> indegree = find 
                    } else {
                        break;
                    }
                }
                ret.put(entry.getKey(), mapid);
            }
        }
        return ret;
    }

    private HashMap<Integer, ArrayList<ArrayList<Integer>>> indegreeMap(Set<ArrayList<Integer>> keySet) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();
        int sumde = 0;

//System.out.println(keySet.size());
        //      for (ArrayList<ArrayList<Integer>> temp : keySet) {
        for (ArrayList<Integer> arr : keySet) {
            int sumt = 0;
            for (int i : arr) {
                sumt += i;
            }
            if (!map.containsKey(Math.abs(sumt - sumde))) {
                map.put(Math.abs(sumt - sumde), new ArrayList<ArrayList<Integer>>());
            }
            map.get(Math.abs(sumt - sumde)).add(arr);
        }
     //   }

        //  System.out.println(map.size());
        return map;

    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> copyRefer(HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs1) {
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry : referencingIDs1.entrySet()) {
            HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> map = new HashMap<>();
            for (Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
                ArrayList<Integer> arr = new ArrayList<>();
                for (Integer s : entry2.getValue()) {
                    arr.add(s);
                }
                map.put(entry2.getKey(), arr);
            }
            referencingIDs.put(entry.getKey(), map);

        }
        return referencingIDs;
    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> copyUpdate(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree1) {
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> referencingIDs = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> entry : updatedDegree1.entrySet()) {
            HashMap<ArrayList<Integer>, ArrayList<Integer>> map = new HashMap<>();
            for (Entry<ArrayList<Integer>, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
                ArrayList<Integer> arr = new ArrayList<>();
                for (Integer s : entry2.getValue()) {
                    arr.add(s);
                }
                map.put(entry2.getKey(), arr);
            }
            referencingIDs.put(entry.getKey(), map);

        }
        return referencingIDs;
    }
    int mergeindexcount = 0;

    private ArrayList<Integer> mergeMapCandidate(HashMap<Integer, ArrayList<ArrayList<Integer>>> norm1Map, ArrayList<Integer> arr) {
        int sumde = 0;
        //    for (ArrayList<Integer> arr : pair) {
        for (int i : arr) {
            sumde += i;
        }
        //   }

        int max = Integer.MAX_VALUE;
        for (int i = 0; i < max; i++) {
            if (norm1Map.containsKey(i + sumde) && !norm1Map.get(i + sumde).isEmpty()) {
                mergeindexcount = i + sumde;
                return norm1Map.get(mergeindexcount).get(0);
            }
            if (norm1Map.containsKey(-i + sumde) && !norm1Map.get(-i + sumde).isEmpty()) {
                mergeindexcount = -i + sumde;
                return norm1Map.get(mergeindexcount).get(0);
            }

        }
        System.out.println("error");
        return null;
    }

    private void processAvaMap(HashMap<ArrayList<String>, ArrayList<ArrayList<String>>> avaMaps, HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> tableCorrelationDistribution, HashMap<String, ArrayList<String>> mergedDegreeTitle) {
        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : tableCorrelationDistribution.entrySet()) {
            ArrayList<ArrayList<String>> attributeOrders = new ArrayList<>();
            ArrayList<ArrayList<String>> avaAttributes = new ArrayList<>();
            for (int i = 1; i < entry.getKey().size(); i++) {
                ArrayList<String> arr1 = new ArrayList<>();
                arr1.add(entry.getKey().get(i));
                arr1.addAll(mergedDegreeTitle.get(entry.getKey().get(i)));
                attributeOrders.add(arr1);
                ArrayList<String> arr = new ArrayList<>();
                arr.add(entry.getKey().get(i));
                arr.add(entry.getKey().get(0));
                avaAttributes.add(arr);
            }
            avaMaps.put(entry.getKey(), avaAttributes);
        }
    }

    private HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> corrMap(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution, HashMap<ArrayList<String>, HashMap<Integer, Integer>> downsizedCounts) {
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> result = new HashMap<>();
        //   HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> result= new HashMap<>();
        ArrayList<MergeVertex> liss = new ArrayList<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : mergedDistribution.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> arrs = new ArrayList<>();
            for (int i = 1; i < entry.getKey().size(); i++) {
                ArrayList<String> arr = new ArrayList<>();
                arr.add(entry.getKey().get(0));
                arr.add(entry.getKey().get(i));
                arrs.add(downsizedCounts.get(arr));
            }
            MergeVertex mergeVertex = new MergeVertex(result, entry.getKey(), arrs, entry.getValue());
            settleCorrVetex(mergeVertex);
            liss.add(mergeVertex);

            System.out.println("start");
            //      System.out.println(entry.getKey());
            (new Thread(mergeVertex)).start();
       //   HashMap<ArrayList<Integer>, Integer> correlated= produceCorr(entry.getValue(),arrs);

            //    result.put(entry.getKey(), correlated);
            System.out.println("end");
        }

        boolean sig = true;
        while (sig) {
            sig = false;
            for (MergeVertex k : liss) {
                if (!k.stop) {
                    sig = true;
                    break;
                }
            }

        }
        return result;
    }

    private void disMapComp(HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution) {
        ArrayList<ParaDisComp> liss = new ArrayList<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : avaCounts.entrySet()) {
            ParaDisComp pdc = new ParaDisComp(distanceMap, entry);
            liss.add(pdc);
            (new Thread(pdc)).start();
        }
        boolean sig = true;
        while (sig) {
            sig = false;
            for (ParaDisComp k : liss) {
                if (!k.stop) {
                    sig = true;
                    break;
                }
            }

        }
    }
    ArrayList<Integer> curIndexes;
    HashMap<ArrayList<String>, List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>> sortedCorrs = new HashMap<>();

    private void sortCorrelation(HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> tableCorrelationDistribution, HashMap<String, ArrayList<String>> mergedDegreeTitle) {
        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : tableCorrelationDistribution.entrySet()) {
            ArrayList<ArrayList<String>> attributeOrders = new ArrayList<>();
            ArrayList<ArrayList<String>> avaAttributes = new ArrayList<>();
            curIndexes = new ArrayList<>();
            for (int i = 1; i < entry.getKey().size(); i++) {
                ArrayList<String> arr1 = new ArrayList<>();
                arr1.add(entry.getKey().get(i));
                arr1.addAll(mergedDegreeTitle.get(entry.getKey().get(i)));
                curIndexes.add(mergedDegreeTitle.get(entry.getKey().get(i)).indexOf(entry.getKey().get(0)));
                attributeOrders.add(arr1);
                ArrayList<String> arr = new ArrayList<>();
                arr.add(entry.getKey().get(i));
                arr.add(entry.getKey().get(0));
                avaAttributes.add(arr);
            }
            Sort s = new Sort();
            System.out.println(entry.getKey());
            List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> list = s.sortOnKeyPosition(entry.getValue(), this.curIndexes);
            List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> list2 = new ArrayList<>();
            if (entry.getKey().get(0).equals("socialgraph")) {
                int start = 0;
                HashSet<Integer> idArr = new HashSet<>();
                while (start < list.size()) {
                    Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 = list.get(start);
                    if (this.sumVector(entry2.getKey().get(0)) > this.sumVector(entry2.getKey().get(1))) //  list.remove(start);
                    {
                        idArr.add(start);
                    }
                    start++;
                  //      System.out.println(start+" "+list.size());

                }

                start = 0;
                while (start < list.size()) {
                    if (!idArr.contains(start)) {
                        list2.add(list.get(start));
                    }
                    start++;
                }
                System.out.println(list2.size());
                list = list2;
                System.out.print(list.size());
            }
            sortedCorrs.put(entry.getKey(), list);

            //   System.out.println(avaCounts);
        }
    }

    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }
}
