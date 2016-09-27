/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.CoDa;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jiangwei
 *
 * This is most update correlation function for tweets and followee, and
 * follower
 */
public class CorrelationVertex1 extends PrintFunction {

    /**
     * @param args the command line arguments
     */
    public double stime = 0.2;
    double rationP = 0.005;
    String curTable = "";
    ArrayList<Integer> curIndexes = new ArrayList<>();
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> target = new HashMap<>();
    public boolean eveNum = false;

    HashMap<String, List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>> sortedCorrs = new HashMap<>();
    HashMap<String, ArrayList<ComKey>> referenceTable;
    HashMap<String, Integer> scaleTableSize;
    HashMap<String, Integer> oldTableSize;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree;
    HashMap<String, Boolean> uniqueNess;
    CoDa originalCoDa;

    public CorrelationVertex1() {
    }

   int indexcount = 0;

  
   

    int iteration = 0;

    HashMap<ArrayList<ArrayList<Integer>>, Integer> allBounds = new HashMap<>();

    HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap = new HashMap<>();

 
  
    boolean posValue = false;
    HashSet<ArrayList<ArrayList<Integer>>> boundTrash = new HashSet<>();

    
   int totalSum = 0;
    Sort so = new Sort();

  
     List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<>();

    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> corrDist
        (HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, 
                HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> tableCorrelationDistribution, 
                HashMap<String, ArrayList<ComKey>> mergedDegreeTitle, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio, HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts) {
        
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result = new HashMap<>();
        target = downsizedMergedDistribution;
        
        ArrayList<Thread> liss = new ArrayList<>();
        System.out.println(this.uniqueNess);
        for (Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : tableCorrelationDistribution.entrySet()) {
            curTable = entry.getKey();
            if (curTable.equals("socialgraph")) {
                eveNum = true;
            }
            //   System.out.println(avaCounts);
            if (eveNum){
                for (Entry<ArrayList<ArrayList<Integer>>, Integer> entryv : entry.getValue().entrySet()){
                    ArrayList<ArrayList<Integer>> pair = new ArrayList<>();
                    pair.add(entryv.getKey().get(1));
                    pair.add(entryv.getKey().get(0));
                    if (!entry.getValue().containsKey(pair) ||  !entry.getValue().get(pair).equals(entryv.getValue())){
                        System.out.println(entryv);
                        System.out.println(entry.getValue().get(pair));
                        System.exit(-1);
                    }
                }
                
            }
            ParaCorrMap pcm = new ParaCorrMap(this.distanceMap,result,avaCounts, entry.getValue(),downsizedMergedDistribution, downsizedMergedRatio, mergedDegreeTitle);
            
          //  pcm.sorted = sortedCorrs.get(entry.getKey());
            pcm.originalCoDa = this.originalCoDa;
            pcm.eveNum = this.eveNum;
            pcm.stime = this.scaleTableSize.get(curTable)*1.0/this.oldTableSize.get(this.curTable);
            pcm.target = this.target;
            pcm.curTable = curTable;
            pcm.referenceTable = this.referenceTable;
            pcm.mappedBestJointDegree = this.mappedBestJointDegree;
            pcm.uniqueNess = this.uniqueNess;
           // if (pcm.sorted == null) {
           //     System.out.println("Null:" + sortedCorrs.keySet());
           //     System.out.println("Null:" + entry.getKey());
            //}
            Thread thr = new Thread(pcm);
            liss.add(thr);
            thr.start();
            eveNum = false;
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

}
