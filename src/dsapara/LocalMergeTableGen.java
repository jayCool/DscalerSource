/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.DB;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author workshop
 */
public class LocalMergeTableGen implements Runnable {

    Map.Entry<String, HashMap<ArrayList<Integer>, AvaStat>> entry;
    HashMap<String, HashMap<Integer, String>> result;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseMergedDegree;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    String outFile;
    double s;
    DB originalDB;

    private ArrayList<ArrayList<Integer>> l1Norm2Sets(ArrayList<Integer> pair, Set<ArrayList<Integer>> keySet) {
        int maxABS = 0;

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();

        for (ArrayList<Integer> arr : keySet) {
            int diff = 0;
            for (int i = 0; i < pair.size() && arr.size() >= pair.size(); i++) {
                diff += Math.abs(pair.get(i) - arr.get(i));
            }
            maxABS = Math.max(maxABS, Math.abs(diff));

            if (!map.containsKey(Math.abs(diff))) {
                map.put(Math.abs(diff), new ArrayList<ArrayList<Integer>>());
            }
            map.get(Math.abs(diff)).add(arr);
        }

        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i <= maxABS; i++) {
            if (map.containsKey(i)) {
                result.addAll(map.get(i));
                break;
            }
        }
        return result;

    }

    @Override
    public void run() {
        FileWriter writer = null;
        try {
            ArrayList<ArrayList<Integer>> calDegs = new ArrayList<>();
            ArrayList<Integer> calDeg = new ArrayList<>();
            //  HashMap<Integer, String> mmap = new HashMap<>();
            ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> tempH = new ConcurrentHashMap<>();
            File file = new File(outFile + "/" + "local_s_" + this.s + entry.getKey() + ".txt");
            writer = new FileWriter(file);
            BufferedWriter pw = new BufferedWriter(writer, 100000);
            int sum = 0;
            for (Map.Entry<ArrayList<Integer>, AvaStat> entry2 : entry.getValue().entrySet()) {
                ArrayList<Integer> arr = new ArrayList<>();
                for (int t : entry2.getValue().ids) {
                    arr.add(t);
                }
                sum += entry2.getValue().ids.length;
                tempH.put(entry2.getKey(), arr);
            }
            
            int level = 0;
            int tableNum = this.originalDB.getTableNum(entry.getKey());
            int printed = 0;
            while (tempH.keySet().size() > 0) {
                for (Map.Entry<ArrayList<Integer>, ArrayList<Integer>> entry2 : tempH.entrySet()) {
                    int leftOver = entry2.getValue().size();
                    calDeg = entry2.getKey();

                    int st = 0;
                    if (level == 0) {
                        calDeg = entry2.getKey();
                        if (reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).containsKey(calDeg) &&
                                reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg).size() > 0) {
                            
                            int size = reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg).size();
                            ArrayList<Integer> oldIDs = new ArrayList<>();
                            for (int i : reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg)) {
                                oldIDs.add(i);
                            }
                            Collections.shuffle(oldIDs);
                            for (int i = 0; i < leftOver; i++) {
                                pw.write("" + entry2.getValue().get(i));
                                int pkidNum = oldIDs.get(i % size);
                                if (this.originalDB.tables[tableNum].nonKeys[pkidNum] != null) {
                                    pw.write("|" + this.originalDB.tables[tableNum].nonKeys[pkidNum]);
                                }
                                printed ++ ;
                                pw.newLine();
                            }
                            tempH.remove(entry2.getKey());
                        }
                    } else {
                        calDegs = l1Norm2Sets(entry2.getKey(), reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).keySet());

                        calDeg = calDegs.get((int) (Math.random() * (calDegs.size() - 1) + 0.49));
                        boolean check = false;
                        while (reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg).isEmpty()) {
                            reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).remove(calDeg);
                            st++;
                            if (calDegs.size() == 0) {
                                check = true;
                                break;
                            }

                            calDeg = calDegs.get((int) (Math.random() * (calDegs.size() - 1) + 0.49));
                            calDegs.remove(calDeg);
                        }
                        if (check) {
                            continue;
                        }
                        int size = reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg).size();
                        ArrayList<Integer> oldIDs = new ArrayList<>();
                        for (int i : reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg)) {
                            oldIDs.add(i);
                        }
                        Collections.shuffle(oldIDs);
                        for (int i = 0; i < leftOver; i++) {
                            //  mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
                            pw.write("" + entry2.getValue().get(i));
                            // int pkidNum = this.originalDB.tables[tableNum].pkMapping.get(oldIDs.get(i % size));
                            int pkidNum = oldIDs.get(i % size);
                            if (this.originalDB.tables[tableNum].nonKeys[pkidNum] != null) {
                                pw.write("|" + this.originalDB.tables[tableNum].nonKeys[pkidNum]);
                            }
                            
                            printed ++;

                            pw.newLine();
                        }
                        tempH.remove(entry2.getKey());
                    }

                }
                System.out.println(this.entry.getKey() + "    printed:" + printed );
                level++;
            }
            pw.close();
            //      result.put(entry.getKey(), mmap);
        } catch (IOException ex) {
            Logger.getLogger(LocalMergeTableGen.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                writer.close();
            } catch (IOException ex) {
                Logger.getLogger(LocalMergeTableGen.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
