/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author workshop
 */
public class OneKeyCombine implements Runnable {

    public String uidTweet = "fiddegree.txt";
    public int sourceAfter = 60704;
    public int secondPoint = 0;
    private double randomProb = 0.5;
    public double oneRatio = 1;
    public int domaingap = 0;
    public int breakIn = 0;
    public double s = 0.8;
    public int calSourceAfter = 0;
    public boolean evenNum = false;
    public int dependAfter = 411958;
    /**
     * @param args the command line arguments
     */
    int untouched = 0;
    public HashMap<ComKey, HashMap<Integer, Integer>> result;
    public HashMap<String, Integer> scaleCounts;
    HashMap<Integer, Integer> orders;
    public ComKey key;

    public boolean stoped;

    OneKeyCombine(HashMap<ComKey, HashMap<Integer, Integer>> result, HashMap<String, Integer> scaleCounts, ComKey key, HashMap<Integer, Integer> orders) {
        this.scaleCounts = scaleCounts;
        this.result = result;
        this.orders = orders;
        this.key = key;
    }

    public HashMap<Integer, Integer> runA(HashMap<Integer, Integer> orders) throws FileNotFoundException {
        HashMap<Integer, Integer> downsizeDegree = downSizeDstats(orders);

        HashMap<Integer, Integer> smoothDegree = smoothDstat_DBScale(downsizeDegree, dependAfter, this.sourceAfter);
        if (Collections.min(smoothDegree.values()) < 0) {
            System.out.println(this.key + "   Error: " + smoothDegree);
            System.exit(-1);
        }
        TreeMap<Integer, Integer> map1 = new TreeMap<>(orders);
        TreeMap<Integer, Integer> map2 = new TreeMap<>(smoothDegree);
        System.out.println(this.key + "DS-statistics: " + this.getDStatistics(map2, map1));
        return smoothDegree;
    }
    HashMap<Integer, ArrayList<Integer>> map = new HashMap<>();

    HashMap<Integer, Integer> smoothDstat_DBScale(HashMap<Integer, Integer> downsizeDegree, int dependAfter, int sourceAfter) {
        ArrayList<Integer> x = new ArrayList<Integer>();

        x.addAll(downsizeDegree.keySet());
        int max = Collections.max(x);
        ArrayList<Integer> value = new ArrayList<>();
        untouched = (int) Math.min(value.size() * 0.1, untouched);
        untouched = (int) (Math.random() * untouched);
        x = new ArrayList<>();
        for (int i = 0; i <= max; i++) {
            if (downsizeDegree.containsKey(i)) {
                value.add(downsizeDegree.get(i));
            } else {
                value.add(0);
            }
            x.add(i);
        }

        System.out.println("X size: " + x.size() + "   " + this.key);

        /*if (x.size() == 1) {
         for (int i = 0; i < x.get(x.size() - 1); i++) {
         x.add(i, i);
         value.add(i, 0);
         }
         for (int i = 0; i < 5; i++) {
         x.add(x.size());
         value.add(0);
         }
         }*/
        int vtex = this.sumVector(value);
        int diffs = sourceAfter - vtex;
        if (Collections.min(value) < 0) {
            System.out.print("Downsize Adjustment: " + value);
        }

        System.out.println(key + "===================node adjust" + diffs + "=====================");

        for (int i = 0; i <= 5 && i < x.size() & diffs != 0; i++) {
            double rati = (int) 1.0 * value.get(i) / vtex;
            value.set(i, Math.max(0, (int) (rati * diffs) + value.get(i)));
            if (value.get(i) < 0) {
                int k = -value.get(i);
                for (int j = 0; j < value.size() && k > 0; j++) {
                    if (value.get(j) > 0) {
                        value.set(i, value.get(i) + 1);
                        k--;
                        value.set(j, value.get(j) - 1);
                    }
                }
            }
        }

        vtex = this.sumVector(value);
        diffs = sourceAfter - vtex;

        System.out.println("value check: " + diffs);
        int kk = 0;

        while (diffs != 0) {
            if (kk == value.size() - 1) {
                kk = 0;
            }
            value.set(kk, Math.max(0, value.get(kk) + Math.abs(diffs) / diffs));
            kk++;
            vtex = this.sumVector(value);
            diffs = sourceAfter - vtex;
        }
        System.out.println(key + "=================nodeAdjustment1 done" + diffs + "======================");

        int products = product(x, value);
        System.out.println(value);
        System.out.println(this.key + "products: " + products + " dependAfter: " + dependAfter);
        int diff = products - dependAfter;
        int sign = 1;
        boolean flag = true;
        if (diff > 0) {
            sign = -1;
            flag = false;
        }
        int iin = 0;
        for (int i = 0; i < value.size(); i++) {
            if (value.get(i) > 0 && x.get(i) == i + value.get(i)) {
                iin = i;
            } else {
                break;
            }
        }

        products = product(x, value);
        diff = dependAfter - products;
        System.out.println(this.key + "   " + (1.0 * dependAfter / products));
        if ((1.0 * dependAfter / products) > 1.1 && diff > 0) {
            int largest = x.get(x.size() - 1);
            for (int i = largest + 1; i < largest * (1.0 * dependAfter / products); i++) {
                x.add(i);
                value.add(0);
            }
        }

        System.out.println(key + "   adjusted size: " + x.size());
        if (Collections.min(value) < 0) {
            System.out.print("Node Adjustment: " + value);
        }

        map = this.maximumRange(x, value, diff);

        diff = smoothingLoop(diff, value, x);

        products = product(x, value);
        System.out.println(this.key + "   Before: " + products);
        if (diff != 0) {
            ArrayList<Integer> arr = map.get(diff);
            value.set(arr.get(0), value.get(arr.get(0)) - 1);
            value.set(arr.get(1), value.get(arr.get(1)) + 1);
        }

        products = product(x, value);
        System.out.println(this.key + "   After: " + products);
        products = product(x, value);
        HashMap<Integer, Integer> res = new HashMap<>();
        for (int i = 0; i < x.size(); i++) {
            res.put(x.get(i), value.get(i));
        }
        this.calSourceAfter = sumVector(value);

        System.out.println(this.key + "===============Tuple Adjustment 3 Done=========================" + products);
        return res;
    }

    //load the degree dsitribution, key is the degree, value is the degree frequency
    private double getDStatistics(TreeMap<Integer, Integer> sample, TreeMap<Integer, Integer> real) {
        double results = 0;
        TreeMap<Double, Double> pmf1 = new TreeMap<>();
        TreeMap<Double, Double> pmf2 = new TreeMap<>();

        //   sample.remove(0.0);
        //  real.remove(0.0);
        int sum1 = 0;
        for (Map.Entry<Integer, Integer> entry : sample.entrySet()) {
            sum1 += entry.getValue();
        }
        int sum2 = 0;
        for (Map.Entry<Integer, Integer> entry : real.entrySet()) {
            sum2 += entry.getValue();
        }

        for (Map.Entry<Integer, Integer> entry : real.entrySet()) {
            //  Pair pair = new Pair(entry.getKey(),1.0*entry.getValue()/sum2);
            pmf2.put(1.0 * entry.getKey(), 1.0 * entry.getValue() / sum2);
        }

        for (Map.Entry<Integer, Integer> entry : sample.entrySet()) {
            //      Pair pair = new Pair(entry.getKey(),1.0*entry.getValue()/sum1);
            pmf1.put(1.0 * entry.getKey(), 1.0 * entry.getValue() / sum1);
        }
//pmf1.
        for (Map.Entry<Double, Double> entry : pmf1.entrySet()) {
            if (!pmf2.containsKey(entry.getKey())) {
                pmf2.put(entry.getKey(), 0.0);
            }
        }

        for (Map.Entry<Double, Double> entry : pmf2.entrySet()) {
            // System.out.println(entry.getKey()+"ssss");
            if (!pmf1.containsKey(entry.getKey())) {
                pmf1.put(entry.getKey(), 0.0);
            }
        }

        ArrayList<Double> cmf1 = new ArrayList<>();
        ArrayList<Double> cmf2 = new ArrayList<>();
        int i = 0;
        for (Map.Entry<Double, Double> entry : pmf2.entrySet()) {
            if (i != 0) {
                cmf1.add(pmf1.get(entry.getKey()) + cmf1.get(i - 1));
                cmf2.add(entry.getValue() + cmf2.get(i - 1));

            } else {
                cmf1.add(pmf1.get(entry.getKey()));
                cmf2.add(entry.getValue());

            }
            i++;
        }
        double max = 0;
        for (int t = 0; t < cmf1.size(); t++) {
            if (max < Math.abs(cmf1.get(t) - cmf2.get(t))) {
                max = Math.abs(cmf1.get(t) - cmf2.get(t));
            }
        }
        return max;
    }

    private HashMap<Integer, Integer> downSizeDstats(HashMap<Integer, Integer> orders) {
        HashMap<Integer, Integer> results = new HashMap<>();
        ArrayList<Integer> resultsa = new ArrayList<>();
        double accu = 0;
        int total = 0;
        int used = 0;
        for (Entry<Integer, Integer> entry : orders.entrySet()) {
            total += entry.getValue();
            if (entry.getValue() * this.s < 1) {
                double prob = Math.random();
                resultsa.add(entry.getKey());
                if (1.0 * entry.getValue() * this.s > prob) {
                    used++;
                    results.put(entry.getKey(), 1);
                }
                accu += 1.0 * entry.getValue() * this.s;
            } else {
                resultsa.add(entry.getKey());
                double prob = Math.random();
                double gap = entry.getValue() * this.s - (int) (entry.getValue() * this.s);
                if (gap > prob) {
                    used += (int) (entry.getValue() * this.s) + 1;
                    results.put(entry.getKey(), (int) (entry.getValue() * this.s) + 1);
                } else {
                    results.put(entry.getKey(), (int) (entry.getValue() * this.s));
                    used += (int) (entry.getValue() * this.s);
                }
            }
        }

        System.out.println("original: " + orders);
        System.out.println("simple scaled: " + results);

        if (this.s < 1) {
            Collections.sort(resultsa);

            double interval = (1.0 * (resultsa.size() - 2) / (total * this.s - used));
            for (int i = 0; i < total * this.s - used; i++) {
                int temp = resultsa.get((int) (i * interval));
                if (results.containsKey(temp)) {
                    results.put(temp, results.get(temp) + 1);
                } else {
                    results.put(temp, 1);
                }
            }
        }

        System.out.println(this.key + "after: " + sourceAfter + " " + dependAfter);
        untouched = 0;
        return results;
    }

    //Large scale degree is considered
    HashMap<Integer, Integer> smoothDstat(HashMap<Integer, Integer> downsizeDegree, int dependAfter1, int sourceAfter1) {
        ArrayList<Integer> x = new ArrayList<Integer>();
        if (this.evenNum) {
            dependAfter = dependAfter + dependAfter % 2;
        }

        x.addAll(downsizeDegree.keySet());
        Collections.sort(x);
        ArrayList<Integer> value = new ArrayList<>();
        untouched = (int) Math.min(value.size() * 0.1, untouched);

        untouched = (int) (Math.random() * untouched);

        for (int key : x) {
            value.add(downsizeDegree.get(key));
        }
        if (x.size() == 1) {
            for (int i = 0; i < x.get(x.size() - 1); i++) {
                x.add(i, i);
                value.add(i, 0);
            }
            for (int i = 0; i < 5; i++) {
                x.add(x.size());
                value.add(0);
            }
        }
        int vtex = this.sumVector(value);
        //     System.out.println(x);
        //    System.out.println(value);
        int diffs = sourceAfter - vtex;
        for (int i = 0; i <= 5 && i < x.size(); i++) {
            double rati = (int) 1.0 * value.get(i) / vtex;
            value.set(i, Math.max(0, (int) (rati * diffs) + value.get(i)));
            if (value.get(i) < 0) {
                int k = -value.get(i);
                for (int j = 0; j < value.size() && k > 0; j++) {
                    if (value.get(j) > 0) {
                        value.set(i, value.get(i) + 1);
                        k--;
                        value.set(j, value.get(j) - 1);
                    }
                }
            }
        }
        vtex = this.sumVector(value);
        diffs = sourceAfter - vtex;
        int kk = 0;

        while (diffs != 0) {
            if (kk == value.size() - 1) {
                kk = 0;
            }
            value.set(kk, Math.max(0, value.get(kk) + Math.abs(diffs) / diffs));
            kk++;
            //     System.out.println("diffs"+diffs);
            vtex = this.sumVector(value);
            diffs = sourceAfter - vtex;
        }

        int products = product(x, value);
        int diff = products - dependAfter;
        int sign = 1;
        boolean flag = true;
        if (diff > 0) {
            sign = -1;
            flag = false;
        }
        int start = 0;
        int ender = value.size() - untouched - 1;
        int iin = 0;
        for (int i = 0; i < value.size(); i++) {
            if (value.get(i) > 0 && x.get(i) == i + value.get(i)) {
                iin = i;
            } else {
                break;
            }
        }

        products = product(x, value);
        diff = products - dependAfter;
        diff = -diff;
        int starter = 0;
        HashMap<Integer, ArrayList<Integer>> map = this.maximumRange(x, value, diff);
        while (!map.containsKey(diff)) {
            if (diff < 0) {
                if (value.get(ender) > 0) {
                    value.set(starter, value.get(starter) + 1);
                    value.set(ender, value.get(ender) - 1);
                    starter++;
                    ender--;
                } else {
                    ender--;
                }
            } else if (value.get(starter) > 0) {
                value.set(starter, value.get(starter) - 1);
                value.set(ender, value.get(ender) + 1);
                starter++;
                ender--;
            } else {
                starter++;
            }
            if (starter >= ender) {
                starter = 0;
                ender = value.size() - untouched - 1;
            }

            products = product(x, value);
            diff = dependAfter - products;
            map = this.maximumRange(x, value, diff);
        }

        if (diff != 0) {
            ArrayList<Integer> arr = map.get(diff);
            value.set(arr.get(0), value.get(arr.get(0)) - 1);
            value.set(arr.get(1), value.get(arr.get(1)) + 1);

        }
        products = product(x, value);

        HashMap<Integer, Integer> res = new HashMap<>();
        for (int i = 0; i < x.size(); i++) {
            res.put(x.get(i), value.get(i));
        }
        this.calSourceAfter = sumVector(value);
        //  System.out.println(value);
        return res;
    }

    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }

    private HashMap<Integer, ArrayList<Integer>> maximumRange(ArrayList<Integer> x, ArrayList<Integer> value, int diff) {
        this.usedIndex = new HashSet<>();
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<>();
        if (diff > 0) {
            for (int i = 0; i < x.size(); i++) {
                for (int j = 0; j < i; j++) {
                    if (value.get(j) > 0) {
                        ArrayList<Integer> arr = new ArrayList<>();
                        arr.add(j);
                        arr.add(i);
                        result.put(x.get(i) - x.get(j), arr);
                    }
                }
            }
        } else {
            for (int i = 0; i < x.size(); i++) {
                if (value.get(i) > 0) {
                    for (int j = 0; j < i; j++) {
                        ArrayList<Integer> arr = new ArrayList<>();
                        arr.add(i);
                        arr.add(j);
                        result.put(x.get(j) - x.get(i), arr);
                    }
                }

            }
        }
        return result;
    }

    private int product(ArrayList<Integer> x, ArrayList<Integer> value) {
        int sum = 0;
        for (int i = 0; i < x.size(); i++) {
            if (x.get(i) > 0 && value.get(i) > 0) {
                sum += x.get(i) * value.get(i);
            }
        }
        return sum;
    }

    /*   HashMap<Integer, Integer> runB(HashMap<Integer, Integer> orders) {
     HashMap<Integer, Integer> downsizeDegree = downSizeDstats(orders);
     HashMap<Integer, Integer> smoothDegree = smoothDstat(downsizeDegree, dependAfter, this.sourceAfter);
     for (Entry<Integer, Integer> entry : smoothDegree.entrySet()) {
     if (entry.getValue() < 0) {
     System.out.println(entry);
     }
     }

     TreeMap<Integer, Integer> map1 = new TreeMap<>(orders);
     TreeMap<Integer, Integer> map2 = new TreeMap<>(smoothDegree);
     System.out.println("DS-statistics: " + this.getDStatistics(map2, map1));
     return smoothDegree;

     }
     */
    @Override
    public void run() {
        try {
            result.put(key, this.runA(orders));
            scaleCounts.put(key.referencingTable, dependAfter);
            scaleCounts.put(key.sourceTable, sourceAfter);
            stoped = true;

        } catch (FileNotFoundException ex) {
            Logger.getLogger(OneKeyCombine.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    boolean changed = false;
    boolean edited = false;
    int starter = 0;
    int ender = 0;
    boolean maxflag = false;
    HashSet<Integer> usedIndex = new HashSet<>();

    private int smoothingLoop(int diff, ArrayList<Integer> value, ArrayList<Integer> x) {
        ender = value.size() - untouched - 1;

        HashSet<Integer> history = new HashSet<>();
        int loop = 0;
        while (!map.containsKey(diff) && diff != 0) {
            if (loop % 1000 == 0) {
                System.out.println(this.key + "    leftOver: " + diff);
                loop = 0;
                int products = product(x, value);
                diff = dependAfter - products;
             //   System.out.println(this.key + "  diff: " + diff+"  stater: " + starter + "  end: " + ender + "   " + value.subList(0, 10));

            }
            loop++;
            //  diff = straightCut(diff, x, value, history);

            if (diff == 0) {
                break;
            }

            //  diff = expandIndex(diff, history, x, value);
            diff = twowayShrink(diff, x, value);
            //   System.out.println(key + "==========FK ADJUSTMENT===============" + diff + "            " + x + "           " + value);
            if (value.get(0) < 0) {
                System.exit(-1);
            }
            
            if (maxflag) {
                map = this.maximumRange(x, value, diff);
                maxflag = false;
            }

            if (Math.abs(diff) < 10000) {
                history.add(diff);
            }
        }

        if (Collections.min(value) < 0) {
            System.out.println("Something is wrong is the looping " + this.key);
        }
        return diff;
    }

    private int findHead(ArrayList<Integer> x) {
        for (int i = 0; i < x.size(); i++) {
            if (x.get(i) > 0) {
                return i;
            }
        }
        return 0;
    }

    private int findTail(ArrayList<Integer> x) {
        for (int i = x.size() - 1; i >= 0; i--) {
            if (x.get(i) > 0) {
                return i;
            }
        }
        return 0;
    }

    private int straightCut(int diff, ArrayList<Integer> x, ArrayList<Integer> value, HashSet<Integer> history) {
        if (diff > 0 && history.contains(diff)) {
            int head = findHead(value);
            if (x.get(x.size() - 1) - x.get(head) > diff) {
                changed = true;
                int v = x.get(head) + Math.abs(diff);
                if (x.contains(v)) {
                    value.set(head, value.get(head) - 1);
                    value.set(x.indexOf(v), value.get(x.indexOf(v)) + 1);
                } else {
                    int tempV = 0;
                    for (int i = 0; i < x.size(); i++) {
                        if (x.get(i) > v) {
                            tempV = i;
                            break;
                        }
                    }

                    x.add(tempV, v);
                    value.add(tempV, 1);
                    value.set(head, value.get(head) - 1);
                }
                int products = product(x, value);
                diff = dependAfter - products;
                //     System.out.println(x+"      "+value);

            }
            maxflag = true;

        } else if (diff < 0 && history.contains(diff)) {
            int tail = findTail(value);
            if (x.get(tail) - x.get(0) > Math.abs(diff)) {
                changed = true;
                int v = x.get(tail) - Math.abs(diff);
                if (x.contains(v)) {
                    value.set(tail, value.get(tail) - 1);
                    value.set(x.indexOf(v), value.get(x.indexOf(v)) + 1);
                } else {
                    int tempV = 0;
                    for (int i = 0; i < x.size(); i++) {
                        if (x.get(i) > v) {
                            tempV = i;
                            break;
                        }
                    }

                    value.set(tail, value.get(tail) - 1);
                    x.add(tempV, v);
                    value.add(tempV, 1);
                }
                int products = product(x, value);
                diff = dependAfter - products;
            }
            maxflag = true;
        }

        if (Collections.min(value) < 0) {
            System.out.print("Cut: " + value);
        }
        int products = product(x, value);
        diff = dependAfter - products;
        return diff;
    }

    private int expandIndex(int diff, HashSet<Integer> history, ArrayList<Integer> x, ArrayList<Integer> value) {
        if (!changed && history.contains(diff) && edited) {
            history.clear();
            int idx = 0, idv = 0;
            boolean found = false;
            // System.out.println(x + "     " + value);

            for (int i = x.get(0) - 1; i < x.get(x.size() - 1); i++) {
                if (i < 0) {
                    continue;
                }
                for (int j = 0; j < x.size(); j++) {
                    if (x.get(j) > i && !x.contains(i)) {
                        found = true;
                        idx = j;
                        idv = i;
                        break;
                    }

                }
                if (found) {
                    break;
                }
            }
            edited = false;
            if (found) {
                x.add(idx, idv);
                value.add(idx, 0);

            }
            if (!found) {
                int v = x.get(x.size() - 1);
                x.add(v + 1);
                value.add(0);
            }

            maxflag = true;
        }

        if (Collections.min(value) < 0) {
            System.out.print("Index Expand: " + value);
        }
        return diff;
    }
    int prevEnder = 0;

    private int twowayShrink(int diff, ArrayList<Integer> x, ArrayList<Integer> value) {
        if (diff < 0) {
            if (value.get(ender) > 0) {
                value.set(starter, value.get(starter) + 1);
                value.set(ender, value.get(ender) - 1);
                //   diff += (ender - starter);
                int products = product(x, value);
                diff = dependAfter - products;

                if (value.get(ender) <= 0) {
                    maxflag = true;
                }
                starter++;
                ender--;
                edited = true;
            } else {
                ender--;
            }
        } else if (value.get(starter) > 0) {
            value.set(starter, value.get(starter) - 1);
            value.set(ender, value.get(ender) + 1);
            // diff -= (ender -starter);
            int products = product(x, value);
            diff = dependAfter - products;

            if (value.get(starter) <= 0) {
                maxflag = true;
            }
            starter++;
            ender--;
            edited = true;
        } else {
            starter++;
        }
        if (starter >= ender) {
            starter = 0;
            if (diff < 0) {
                if (prevEnder > 0) {
                    ender = prevEnder;
                    if (value.get(ender) == 0) {
                        ender--;
                        prevEnder = ender;
                    }
                }
                for (int i = value.size() - 1; i >= 0; i--) {
                    if (value.get(i) > 0) {
                        ender = i;
                        prevEnder = ender;
                        break;
                    }
                }
            } else {
                ender = value.size() - untouched - 1;
            }
        }

        if (Collections.min(value) < 0) {
            System.out.print("Two way: " + value);
        }
        return diff;
    }

}
