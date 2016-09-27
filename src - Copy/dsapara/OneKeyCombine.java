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
public HashMap<ArrayList<String>, HashMap<Integer, Integer>> result;
public HashMap<String, Integer> scaleCounts;
HashMap<Integer, Integer> orders;
public ArrayList<String> key;
  
public boolean stoped;
    OneKeyCombine(HashMap<ArrayList<String>, HashMap<Integer, Integer>> result, HashMap<String, Integer> scaleCounts, ArrayList<String> key, HashMap<Integer, Integer> orders) {
        this.scaleCounts=scaleCounts;
        this.result = result;
        this.orders = orders;
        this.key = key;
    }
    
    public HashMap<Integer, Integer> runA(HashMap<Integer, Integer> orders) throws FileNotFoundException {
        HashMap<Integer, Integer> downsizeDegree = downSizeDstats(orders);
        
        HashMap<Integer, Integer> smoothDegree = smoothDstat(downsizeDegree, dependAfter, this.sourceAfter);
        // if (this.evenNum) System.out.println(smoothDegree);
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
        // System.out.println(sample);
        //   System.out.println(cmf1);
        // System.out.println(cmf1.get(1)-cmf2.get(1)+ " === "+pmf2.firstKey());
        for (int t = 0; t < cmf1.size(); t++) {
            if (max < Math.abs(cmf1.get(t) - cmf2.get(t))) {
                max = Math.abs(cmf1.get(t) - cmf2.get(t));
            }
        }
        return max;
    }
    
    
    //Large scale degree is considered
    private HashMap<Integer, Integer> downSizeDstats(HashMap<Integer, Integer> orders) {
        sourceAfter = 0;
        dependAfter = 0;
     //   System.out.println(orders);
        for (Entry<Integer, Integer> entry : orders.entrySet()) {
            dependAfter += entry.getValue() * entry.getKey();
            sourceAfter += entry.getValue();
        }
        ArrayList<Integer> arr = new ArrayList<>();
        arr.addAll(orders.keySet());
        Collections.sort(arr);
        int thresh = 0;
        thresh = arr.get(arr.size() - 1);
        for (int i = arr.size() - 1; i >= arr.size() - 100 && i > 0; i--) {
            
            if (orders.get(arr.get(i)) > 1) {
                break;
            }
            thresh = arr.get(i);
        }
     //   System.out.println("ssss"+thresh);
        System.out.println("before" + sourceAfter + " " + dependAfter);
        
        dependAfter = (int) (dependAfter * this.s);
        sourceAfter = (int) (sourceAfter * this.s);
        System.out.println("after: " + sourceAfter + " " + dependAfter);
        
        HashMap<Integer, Integer> results = new HashMap<>();
        ArrayList<Integer> resultsa = new ArrayList<>();
        double accu = 0;
        int total = 0;
        int used = 0;
        ArrayList<Integer> repts = new ArrayList<>();
        for (Entry<Integer, Integer> entry : orders.entrySet()) {
            total += entry.getValue();
            if (entry.getValue() * this.s < 1) {
                double prob = Math.random();
                resultsa.add(entry.getKey());
                if (1.0 * entry.getValue() * this.s > prob) {
                    used++;
                    if (entry.getKey()> thresh) {
                         int k=     (int) (entry.getKey() * Math.sqrt(s))+(int) (entry.getKey() * Math.sqrt(s))%2;
                  
                        if (results.containsKey(k)) {
                            repts.add(k);
                            results.put(k, 1 + results.get(k));
                        } else {
                            results.put(k, 1);
                        }
                    } else {
                        results.put(entry.getKey(), 1);
                    }
                }
                
                accu += 1.0 * entry.getValue() * this.s;
                
            } else {
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
        Collections.sort(resultsa);
        for (int i=sourceAfter-used;repts.size()>0&&i>0;i--){
         int ind = i % repts.size();
         results.put(repts.get(ind), 1+repts.get(ind));
        }
        untouched = (int) accu;
        untouched = 0;
        return results;
    }
    
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
        if (x.size()==1){
       for (int i=0;i<x.get(x.size()-1);i++){
           x.add(i, i);
           value.add(i, 0);
       }
       for (int i=0;i<5;i++){
       x.add(x.size());
       value.add(0);
       }
        }
        int vtex = this.sumVector(value);
   //     System.out.println(x);
    //    System.out.println(value);
        int diffs = sourceAfter - vtex;
        for (int i = 0; i <=5 && i<x.size(); i++) {
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
        HashMap<Integer, ArrayList<Integer>> map = this.maximumRange(x, value);
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
            } else {
                if (value.get(starter) > 0) {
                    value.set(starter, value.get(starter) - 1);
                    value.set(ender, value.get(ender) + 1);
                    starter++;
                    ender--;
                } else {
                    starter++;
                }
            }
            if (starter >= ender) {
                starter = 0;
                ender = value.size() - untouched - 1;
            }
            
            products = product(x, value);
            diff = dependAfter - products;
            map = this.maximumRange(x, value);
        }
        
        if (diff != 0) {
            ArrayList<Integer> arr = map.get(diff);
            value.set(arr.get(0), value.get(arr.get(0)) - 1);
            value.set(arr.get(1), value.get(arr.get(1)) + 1);
            
        }
        products = product(x, value);
     //   System.out.println(x);
     //   System.out.println(value);
      // System.out.println("calculated: " + products + "   " + dependAfter + " sumV" + sumVector(value));
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
    
    private HashMap<Integer, ArrayList<Integer>> maximumRange(ArrayList<Integer> x, ArrayList<Integer> value) {
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<>();
        for (int i = 0; i < x.size(); i++) {
            if (value.get(i) > 0) {
                for (int j = i + 1; j < x.size(); j++) {
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(i);
                    arr.add(j);
                    result.put(x.get(j) - x.get(i), arr);
                }
                
                for (int j = 0; j < x.size(); j++) {
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(i);
                    arr.add(j);
                    result.put(x.get(j) - x.get(i), arr);
                    
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
    
    HashMap<Integer, Integer> runB(HashMap<Integer, Integer> orders) {
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

    @Override
    public void run() {
        try {
                   result.put(key, this.runA(orders));
            scaleCounts.put(key.get(1), dependAfter);
            scaleCounts.put(key.get(0), sourceAfter);
    stoped=true;
           ;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(OneKeyCombine.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
