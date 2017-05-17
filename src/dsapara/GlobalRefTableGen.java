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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author workshop
 */
public class GlobalRefTableGen implements Runnable {

    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>> reverseDistribution;
    HashMap<String, HashMap<Integer, String>> result = new HashMap<>();
    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> referencingEntry;
    double s;
    ConcurrentHashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> tempH;
    int leftOver = 0;
    int start = 0;
    int sum = 0;
    private boolean dynamic = false;
    int startInt;
    int endInt;
    String table;

    private ArrayList<ArrayList<ArrayList<Integer>>> nearestSum(ArrayList<ArrayList<Integer>> key) {
        int sum1 = getSum(key);
        ArrayList<ArrayList<ArrayList<Integer>>> result = new ArrayList<>();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (mapping.containsKey(sum1 + i)) {
                int size = mapping.get(sum1 + i).size();
                if (size == 0) {
                    mapping.remove(sum1 + i);
                } else {
                    for (ArrayList<ArrayList<Integer>> te : mapping.get(sum1 + i)) {
                        result.add(te);
                    }
                    if (result.size() > 10) {
                        return result;
                    }
                }
            }
            if (mapping.containsKey(sum1 - i)) {
                for (ArrayList<ArrayList<Integer>> te : mapping.get(sum1 - i)) {
                    result.add(te);
                }
                if (result.size() > 10) {
                    return result;
                }
            }
        }
       // System.out.println("NULL");
        return null;
    }
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> mapping = new HashMap<>();
    int maxSum = 0;

    private void sortDis(Set<ArrayList<ArrayList<Integer>>> keySet) {

        for (ArrayList<ArrayList<Integer>> arr : keySet) {
            int sum = 0;
            for (ArrayList<Integer> karr : arr) {
                for (int i : karr) {
                    sum += i;
                }
            }
            if (sum > maxSum) {
                maxSum = sum;
            }
            if (!mapping.containsKey(sum)) {
                mapping.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            mapping.get(sum).add(arr);
        }

    }

    private int getSum(ArrayList<ArrayList<Integer>> calDeg) {
        int sum1 = 0;
        for (ArrayList<Integer> ks : calDeg) {
            for (int i : ks) {
                sum1 += i;
            }
        }
        return sum1;
    }

    private boolean containsCalDegree(ArrayList<ArrayList<Integer>> calDeg) {
        return reverseDistribution.containsKey(calDeg) && reverseDistribution.get(calDeg).size() > 0;
    }

    boolean staticUp = true;

    @Override
    public void run() {

        if (!staticUp) {
            sortDis(reverseDistribution.keySet());
        }
        ArrayList<ArrayList<Integer>> calDeg = new ArrayList<>();
        ArrayList<ArrayList<ArrayList<Integer>>> calDegs = new ArrayList<>();
        HashMap<Integer, String> mmap = new HashMap<>();
        ArrayList<Integer> indexes = new ArrayList<Integer>();
        int tmp = 0;
        int[] referenceSize = new int[referencingEntry.keySet().size()];

        ArrayList<ArrayList<ArrayList<Integer>>> tempKey = new ArrayList<ArrayList<ArrayList<Integer>>>();
        for (ArrayList<ArrayList<Integer>> arr : referencingEntry.keySet()) {
            referenceSize[tmp] = arr.size();
            indexes.add(tmp);
            tempKey.add(arr);
            tmp++;
        }

        int[] countH = new int[indexes.size()];
        int[] countL = new int[indexes.size()];
        int[] reverseSize = new int[indexes.size()];
        int level = 0;
        ArrayList<ArrayList<String>> reverseids = new ArrayList<ArrayList<String>>();

        ArrayList<ArrayList<Integer>> referenceids = new ArrayList<ArrayList<Integer>>();
        // ids = new ArrayList<Integer>()[5];
        for (int i = 0; i < tempKey.size(); i++) {
            reverseSize[i] = reverseDistribution.get(tempKey.get(i)).size();
            countL[i] = (int) Math.ceil(reverseSize[i] * this.s);
            reverseids.add(reverseDistribution.get(tempKey.get(i)));
            referenceids.add(referencingEntry.get(tempKey.get(i)));
        }
        if (staticUp) {
            HashMap<Integer, String> map = new HashMap<>();
            ArrayList<Thread> liss = new ArrayList<>();
          //  System.out.println(table);
            for (int i = 0; i < 10; i++) {
                StaticUpGlobalRefTableGen sug = new StaticUpGlobalRefTableGen();
                sug.countH = countH;
                sug.countL = countL;
                sug.startInt = i * ((int) Math.ceil(tempKey.size() / 10));
                sug.endInt = (i + 1) * ((int) Math.ceil(tempKey.size() / 10));
                sug.indexes = indexes;
                sug.mmap = map;
                sug.referenceSize = referenceSize;
                sug.referenceids = referenceids;
                sug.reverseSize = reverseSize;
                sug.reverseids = reverseids;
                sug.s = s;
              //  sug.tempKey = tempKey;
                Thread thr = new Thread(sug);
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
            result.put(table, map);
       //     System.out.println(table);
        } else {

            while (indexes.size() > 0) {
                int size = indexes.size();
                ArrayList<Integer> temp = new ArrayList<Integer>(indexes);
                for (int t = 0; t < size; t++) {
                    int pos = temp.get(t);
                    start = countH[pos];
                    // int entrySize = reverseDistribution.get(tempKey.get(pos)).size();
                    int entrySize = reverseSize[pos];
                    // leftOver = referencingEntry.get(tempKey.get(pos)).size() - start;
                    leftOver = referenceSize[pos] - start;

                    if (leftOver == 0) {
                        continue;
                    }
                    if (level == 0) {
                        if (containsCalDegree(tempKey.get(pos))) {
                            produceValueShort(countL, countH, mmap, indexes, pos, tempKey.get(pos), entrySize, reverseids.get(pos), referenceids.get(pos));
                        }
                    } else {
                 //       System.out.println("Error");
                        calDegs = nearestSum(tempKey.get(pos));
                        while (leftOver > 0) {
                            if (calDegs.size() == 0) {
                                break;
                            }
                            calDeg = calDegs.remove(0);
                            boolean check = false;
                            while (!this.reverseDistribution.containsKey(calDeg) || reverseDistribution.get(calDeg).isEmpty()) {
                                int sum1 = this.getSum(calDeg);
                                this.mapping.get(sum1).remove(calDeg);
                                reverseDistribution.remove(calDeg);
                                if (calDegs.size() == 0) {
                                    check = true;
                                    break;
                                }
                                calDeg = calDegs.remove(0);
                            }
                            if (check) {
                                break;
                            }
                            produceValueShort(countL, countH, mmap, indexes, pos, tempKey.get(pos), entrySize, reverseids.get(pos), referenceids.get(pos));
                        }
                    }
                }
                level++;
            }
      //      System.out.println(table + "   " + sum);
            result.put(table, mmap);
        }
    }

    private void produceValueShort(int[] countL, int[] countH, HashMap<Integer, String> mmap, ArrayList<Integer> indexes, int pos,
            ArrayList<ArrayList<Integer>> calDeg, int entrySize, ArrayList<String> reverseids, ArrayList<Integer> referenceids) {
        int cap = (int) Math.ceil(entrySize * this.s);
        int oldV = countL[pos];
        int minV = Math.min(oldV, leftOver);
        int q = cap - oldV;

        for (int i = 0; i < minV; i++) {
            q = q % entrySize;
            String rrid = reverseids.get(q);
            mmap.put(referenceids.get(start), rrid);
            q++;
            start++;
        }
        countL[pos] = oldV - minV;

        if (countL[pos] == 0) {
            //  if (dynamic) {
            int sum1 = this.getSum(calDeg);
            this.mapping.get(sum1).remove(calDeg);
            // }
            //reverseDistribution.get(referencingEntry.getKey()).remove(calDeg);
        }
        sum += minV;
        leftOver = leftOver - minV;

        if (leftOver == 0) {
            indexes.remove(new Integer(pos));
        } else {
            countH[pos] = start;
        }
    }

}
