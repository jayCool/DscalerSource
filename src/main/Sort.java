/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main;

import db.structs.ComKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Zhang Jiangwei
 */
public class Sort {
    public Sort(){
    }
    
       public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnKeySumAsec(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                ArrayList<ArrayList<Integer>> arr1 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
                for (ArrayList<Integer> psum:arr1){
                 for (int p:psum){
                     sum1+=p;
                 }
                }
                 for (ArrayList<Integer> psum:arr2){
                 for (int p:psum){
                     sum2+=p;
                 }
                }
                return (sum1 - sum2);
            }
        });
        return sorted;
    }
       
    
    public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnKeySum(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                ArrayList<ArrayList<Integer>> arr1 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
          //      for (int i=0;i++;i)
                for (ArrayList<Integer> psum:arr1){
                 for (int p:psum){
                     sum1+=p;
                 }
                }
                 for (ArrayList<Integer> psum:arr2){
                 for (int p:psum){
                     sum2+=p;
                 }
                }
                return sum1-sum2;
            }
        });
        return sorted;
    }
    ArrayList<Integer> curIndexes;
      public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnKeyPosition(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders)  {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                ArrayList<ArrayList<Integer>> arr1 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
              for (int i=0;i<arr1.size();i++){
                  sum1+=arr1.get(i).get(curIndexes.get(i));
                  sum2+=arr2.get(i).get(curIndexes.get(i));
                  
              }
                return sum2-sum1;
            }
        });
        return sorted;
    }
     public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnKeySumDesc(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                ArrayList<ArrayList<Integer>> arr1 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
                for (ArrayList<Integer> psum:arr1){
                 for (int p:psum){
                     sum1+=p;
                 }
                }
                 for (ArrayList<Integer> psum:arr2){
                 for (int p:psum){
                     sum2+=p;
                 }
                }
                return (sum2 - sum1);
            }
        });
        return sorted;
    }
     
   
    public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnValueAESC(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                return (o1.getValue() -o2.getValue());
            }
        });
        return sorted;
    }
      public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnleftKeySum(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                ArrayList<ArrayList<Integer>> arr1 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
               ArrayList<Integer> psum=arr1.get(0);
                 for (int p:psum){
                     sum1+=p;
                 }
                
                psum=arr2.get(0);
                 for (int p:psum){
                     sum2+=p;
                 }
                
                return (sum2 - sum1);
            }
        });
        return sorted;
    }
  
                public List<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>> sortOnKeySum2D(HashMap<ArrayList<Integer>, ArrayList<Integer>> orders) {
        List<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>> sorted = new ArrayList<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>>() {
            public int compare(Map.Entry<ArrayList<Integer>, ArrayList<Integer>> o1, Map.Entry<ArrayList<Integer>, ArrayList<Integer>> o2) {
                ArrayList<Integer> arr1 = new ArrayList<>();
                ArrayList<Integer> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
             //   for (ArrayList<Integer> psum:arr1){
                 for (int p:arr1){
                     sum1+=p;
                 }
           //     }
           //      for (ArrayList<Integer> psum:arr2){
                 for (int p:arr2){
                     sum2+=p;
                 }
            //    }
                return (sum2 - sum1);
            }
        });
        return sorted;
    }
                
    /**
     *
     * @param orders
     * @return
     */
    public List<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>> sortOnKeySumJD(HashMap<ArrayList<Integer>, ArrayList<Integer>> orders) {
        List<Map.Entry<ArrayList<Integer>,ArrayList<Integer>>> sorted = new ArrayList<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<Integer>, ArrayList<Integer>>>() {
            public int compare(Map.Entry<ArrayList<Integer>,ArrayList<Integer>> o1, Map.Entry<ArrayList<Integer>,ArrayList<Integer>> o2) {
                ArrayList<Integer> arr1 = new ArrayList<>();
                ArrayList<Integer> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
                 for (int p:arr1){
                     sum1+=p;
                 }
                 for (int p:arr2){
                     sum2+=p;
                 }
                return -(sum2 - sum1);
            }
        });
        return sorted;
    }
    
    
     public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnValueMapDec(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                return (o2.getValue() - o1.getValue());
            }
        });
        return sorted;
    }  
    
    
    //descending order
       public List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnValueMap(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders) {
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                return (o2.getValue() - o1.getValue());
            }
        });
        return sorted;
    }  
      public List<Map.Entry<Integer, Integer>> sortOnValueInteger(HashMap<Integer, Integer> orders) {
        List<Map.Entry<Integer, Integer>> sorted = new ArrayList<Map.Entry<Integer, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return (o2.getValue() - o1.getValue());
            }
        });
        return sorted;
    }
      
       public List<Map.Entry<ArrayList<Integer>, Integer>> sortOnValueIntegerDesc(HashMap<ArrayList<Integer>, Integer> orders) {
        List<Map.Entry<ArrayList<Integer>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<Integer>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<Integer>, Integer>>() {
            public int compare(Map.Entry<ArrayList<Integer>, Integer> o1, Map.Entry<ArrayList<Integer>, Integer> o2) {
                int sum1 = 0, sum2=0;
                for (int i: o1.getKey()){
                    sum1 += i;
                }
                for (int i:o2.getKey()){
                    sum2 += i;
                }
                return (sum2- sum1);
            }
        });
        return sorted;
    }
       
       public List<Map.Entry<Integer, Integer>> sortOnValueAesc(HashMap<Integer, Integer> map) {
        List<Map.Entry<Integer, Integer>> sorted = new ArrayList<Map.Entry<Integer, Integer>>(map.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return (o1.getValue() - o2.getValue());
            }
        });
        return sorted;
    }
       
        public List<Map.Entry<ArrayList<Integer>, Integer>> sortOnValueIntegerAesc(HashMap<ArrayList<Integer>, Integer> orders) {
        List<Map.Entry<ArrayList<Integer>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<Integer>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<Integer>, Integer>>() {
            public int compare(Map.Entry<ArrayList<Integer>, Integer> o1, Map.Entry<ArrayList<Integer>, Integer> o2) {
                return (o1.getValue() - o2.getValue());
            }
        });
        return sorted;
    }
      
         public List<Map.Entry<String, Integer>> sortOnValueIntegerS(HashMap<String, Integer> orders) {
        List<Map.Entry<String, Integer>> sorted = new ArrayList<Map.Entry<String, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return (o1.getValue() - o2.getValue());
            }
        });
        return sorted;
    }
      
             public List<Map.Entry<ArrayList<String>, Integer>> sortOnStringArrayValueIntegerS(HashMap<ArrayList<String>, Integer> orders) {
        List<Map.Entry<ArrayList<String>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<String>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<String>, Integer>>() {
            public int compare(Map.Entry<ArrayList<String>, Integer> o1, Map.Entry<ArrayList<String>, Integer> o2) {
                return (o2.getValue() - o1.getValue());
            }
        });
        return sorted;
    }
                    public List<Map.Entry<ArrayList<Integer>, Integer>> sortOnIntegerArrayValueIntegerS(HashMap<ArrayList<Integer>, Integer> orders) {
        List<Map.Entry<ArrayList<Integer>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<Integer>, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<Integer>, Integer>>() {
            public int compare(Map.Entry<ArrayList<Integer>, Integer> o1, Map.Entry<ArrayList<Integer>, Integer> o2) {
                return (o2.getValue() - o1.getValue());
            }
        });
        return sorted;
    }
             
                public List<Map.Entry<ArrayList<String>, Double>> sortOnStringArrayValueIntegerSDouble(HashMap<ArrayList<String>, Double> orders) {
        List<Map.Entry<ArrayList<String>, Double>> sorted = new ArrayList<Map.Entry<ArrayList<String>, Double>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<String>, Double>>() {
            public int compare(Map.Entry<ArrayList<String>, Double> o1, Map.Entry<ArrayList<String>, Double> o2) {
                return (int)(o2.getValue() - o1.getValue());
            }
        });
        return sorted;
    }
    
    //sort the map based on key in ascending order
    public  List<Map.Entry<Integer, Integer>> sortOnKeyInteger(HashMap<Integer, Integer> orders) {
        List<Map.Entry<Integer, Integer>> sorted = new ArrayList<Map.Entry<Integer, Integer>>(orders.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return (o1.getKey() - o2.getKey());
            }
        });
        return sorted;
    }
    /*
       public List<Map.Entry<Pair, Integer>> sortOnKeyPair(HashMap<Pair, Integer> pairDegree) {
     List<Map.Entry<Pair, Integer>> sorted = new ArrayList<Map.Entry<Pair, Integer>>(pairDegree.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<Pair, Integer>>() {
            public int compare(Map.Entry<Pair, Integer> o1, Map.Entry<Pair, Integer> o2) {
                return (o1.getKey().getSum() - o2.getKey().getSum());
            }
        });
        return sorted; }*/

    List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sortOnKeyPosition(HashMap<ArrayList<ArrayList<Integer>>, Integer> orders, final ArrayList<Integer> curIndexes) {
         List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>(orders.entrySet());
       // System.out.println(sorted);
         Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> o2) {
                ArrayList<ArrayList<Integer>> arr1 = new ArrayList<>();
                ArrayList<ArrayList<Integer>> arr2= new ArrayList<>();
                arr1=o1.getKey();
                arr2=o2.getKey();
                int sum1=0;
                int sum2=0;
              for (int i=0;i<arr1.size();i++){
                  sum1+=arr1.get(i).get(curIndexes.get(i));
              }
              for (int i=0;i<arr2.size();i++){
              sum2+=arr2.get(i).get(curIndexes.get(i));
                  
              }
                return sum2-sum1;
            }
        });
        return sorted;
    }

  

    public List<Map.Entry<ArrayList<ArrayList<Integer>>, Long>> sortOnKeyAppearance(HashMap<ArrayList<ArrayList<Integer>>, Long> rvs,
            final ArrayList<ArrayList<ComKey>> comkeys, HashMap<String, ArrayList<ComKey>> referenceTable) {
    List<Map.Entry<ArrayList<ArrayList<Integer>>, Long>> sorted = new ArrayList<Map.Entry<ArrayList<ArrayList<Integer>>, Long>>(rvs.entrySet());
        Collections.sort(sorted, new Comparator<Map.Entry<ArrayList<ArrayList<Integer>>, Long>>() {
            public int compare(Map.Entry<ArrayList<ArrayList<Integer>>, Long> o1, Map.Entry<ArrayList<ArrayList<Integer>>, Long> o2) {
                
                if (o1.getValue() > o2.getValue()){
                    return 1;
                } 
                if (o1.getValue() < o2.getValue()){
                    return -1;
                } 
                
                else {
                    return 0;
                }
            
            }
        });
        
    return sorted;
    }

}
