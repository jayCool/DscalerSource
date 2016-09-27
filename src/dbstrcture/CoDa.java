/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbstrcture;

import dsapara.ComKey;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author ZhangJiangwei
 */
public class CoDa {
    public HashMap<ComKey, IdFeatures> comKeyMapping = new HashMap<>();
    
    public HashMap<ArrayList<ComKey>, ArrayList<ArrayList<Integer>>> mergedDegrees = new HashMap<>();    
    public HashMap<ArrayList<ComKey>,HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseMergedDegrees = new HashMap<>();    
    public   HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution  = new HashMap<>();
 
    public HashMap<String, ArrayList<ArrayList<ArrayList<Integer>>>> referenceVectors = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseReferenceVectors = new HashMap<>();
     public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> rvCorrelationDis = new HashMap<>();
    
     
        public HashMap<String, ArrayList<ArrayList<ArrayList<Integer>>>> keyVectors = new HashMap<>();
    public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseKeyVectors = new HashMap<>();
     public HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> kvCorrelationDis = new HashMap<>();
     
    public HashMap<ComKey, HashMap<Integer, Integer>> freCounts = new HashMap<>();
 
   // public 
    
}
