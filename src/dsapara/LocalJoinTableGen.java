/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author workshop
 */
public class LocalJoinTableGen implements Runnable {

    Map.Entry<String, HashMap<ArrayList<Integer>,AvaStat>> entry;
    HashMap<String, HashMap<Integer, String>> result;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<String>>> reverseMergedDegree;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;

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

        //  System.out.println(map.size());
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
        ArrayList<ArrayList<Integer>> calDegs = new ArrayList<>();
        ArrayList<Integer> calDeg = new ArrayList<>();

        HashMap<Integer, String> mmap = new HashMap<>();

        //count the frequency of the appeared rrid    
        ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> tempH = new ConcurrentHashMap<>();
        int sum=0;
        for (Map.Entry<ArrayList<Integer>, AvaStat> entry2 : entry.getValue().entrySet()) {
            ArrayList<Integer> arr = new ArrayList<>();
            for (int t : entry2.getValue().ids) {
                arr.add(t);
            }
            sum += entry2.getValue().ids.length;
            tempH.put(entry2.getKey(), arr);
        }
        System.out.println(entry.getKey() + "  " + sum);
        int level = 0;
        while (tempH.keySet().size() > 0) {

            for (Map.Entry<ArrayList<Integer>, ArrayList<Integer>> entry2 : tempH.entrySet()) {
                int leftOver = entry2.getValue().size();
                //
                int st = 0;
                if (level == 0) {
                    calDeg = entry2.getKey();
                    if (reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).containsKey(calDeg) && reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg).size() > 0) {
                        int size = reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg).size();
                        ArrayList<String> oldIDs = new ArrayList<>();
                        for (String i : reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg)) {
                            oldIDs.add(i);
                        }
                        Collections.shuffle(oldIDs);
                        for (int i = 0; i < leftOver; i++) {
                            mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
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
                    ArrayList<String> oldIDs = new ArrayList<>();
                    for (String i : reverseMergedDegree.get(this.mergedDegreeTitle.get(entry.getKey())).get(calDeg)) {
                        oldIDs.add(i);
                    }
                    Collections.shuffle(oldIDs);
                    for (int i = 0; i < leftOver; i++) {
                        mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
                    }
                    tempH.remove(entry2.getKey());
                }

            }
            level++;
        }
        result.put(entry.getKey(), mmap);
    }

}
