/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author workshop
 */
public class ParaMatch implements Runnable {

    HashMap<String, HashMap<Integer, Integer>> ret;
    // HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs;
    HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> updatedDegree;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> biDegreeCorrelation;
    int indexcount = 0;
    int mergeindexcount = 0;
    double s;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> keyVectorIDs;

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

    private HashMap<Integer, ArrayList<ArrayList<Integer>>> indegreeMap(Set<ArrayList<Integer>> keySet) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();
        int sumde = 0;

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
        return map;

    }

    private ArrayList<Integer> mergeMapCandidate(HashMap<Integer, ArrayList<ArrayList<Integer>>> norm1Map, ArrayList<Integer> arr) {
        int sumde = 0;
        for (int i : arr) {
            sumde += i;
        }
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

    private HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> l1NormMap(Set<ArrayList<ArrayList<Integer>>> keySet) {

        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> map = new HashMap<>();
        int sumde = 0;

//System.out.println(keySet.size());
        for (ArrayList<ArrayList<Integer>> temp : keySet) {
            int sumt = 0;

            for (ArrayList<Integer> arr : temp) {
                for (int i : arr) {
                    sumt += i;
                }
            }

            if (!map.containsKey(Math.abs(sumt - sumde))) {
                map.put(Math.abs(sumt - sumde), new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            map.get(Math.abs(sumt - sumde)).add(temp);

        }

        //  System.out.println(map.size());
        return map;

    }

    @Override
    public void run() {

        //  oldMathch();
        newMatch();
    }

    private void newMatch() {
        String table = entry.getKey();

        HashMap<Integer, Integer> mapid = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> referIndex = new HashMap<>();
        HashMap<ArrayList<Integer>, Integer> joinIndex = new HashMap<>();

        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> keyIDs = new HashMap<>();
        int sum = 0;
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> referenceMap = l1NormMap(entry.getValue().keySet());
        HashMap<Integer, ArrayList<ArrayList<Integer>>> degMap = this.indegreeMap(updatedDegree.get(table).keySet());
        while (referenceMap.size() > 0 && degMap.size() > 0) {
            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 : biDegreeCorrelation.get(table).entrySet()) {
                if (referenceMap.size() > 0 && degMap.size() > 0) {
                    int value = (int) (entry2.getValue() * this.s);
                    double pro = Math.random();
                    if (pro < (entry2.getValue() * this.s - (int) (entry2.getValue() * this.s))) {
                        value += 1;
                    }

                    ArrayList<ArrayList<Integer>> refer = new ArrayList<>();
                    for (int i = 1; i < entry2.getKey().size(); i++) {
                        refer.add(entry2.getKey().get(i));
                    }

                    ArrayList<Integer> merge = entry2.getKey().get(0);
                    while (value > 0 && referenceMap.size() > 0 && degMap.size() > 0) {
                        ArrayList<ArrayList<Integer>> calDeg = new ArrayList<>();
                        ArrayList<Integer> calmerge = new ArrayList<>();
                        calDeg = this.findRef(referenceMap, refer);
                        calmerge = this.findMerge(table, merge, degMap);
                        ArrayList<Integer> refIDs = entry.getValue().get(calDeg);
                        int[] mergIDs = updatedDegree.get(table).get(calmerge).ids;

                        if (!referIndex.containsKey(calDeg)) {
                            referIndex.put(calDeg, 0);
                        }

                        if (!joinIndex.containsKey(calmerge)) {
                            joinIndex.put(calmerge, 0);
                        }
                        int it = Math.min(refIDs.size() - referIndex.get(calDeg), mergIDs.length - joinIndex.get(calmerge));

                        it = Math.min(it, value);
                        ArrayList<ArrayList<Integer>> keyVec = new ArrayList<>();
                        keyVec.add(calmerge);
                        keyVec.addAll(calDeg);

                        int refV1 = referIndex.get(calDeg);
                        int joinV2 = joinIndex.get(calmerge);
                        if (!keyIDs.containsKey(keyVec)) {
                            keyIDs.put(keyVec, new ArrayList<Integer>());
                        }
                        for (int i = 0; i < it; i++) {
                            value--;
                            int refid = refIDs.get(i + refV1);
                            int mergid = mergIDs[i + joinV2];
                            mapid.put(mergid, refid);
                            keyIDs.get(keyVec).add(mergid);
                            sum++;
                        }
                        referIndex.put(calDeg, it + refV1);
                        joinIndex.put(calmerge, it + joinV2);

                        if (refIDs.size() == (it + refV1)) {
                            referenceMap.get(indexcount).remove(calDeg);
                            if (referenceMap.get(indexcount).size() == 0) {
                                referenceMap.remove(indexcount);
                            }
                        }

                        if (mergIDs.length == (it + joinV2)) {
                            degMap.get(mergeindexcount).remove(calmerge);
                            if (degMap.get(mergeindexcount).size() == 0) {
                                degMap.remove(mergeindexcount);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
        System.out.println("sum:" + sum);
        this.keyVectorIDs.put(table, keyIDs);
        ret.put(table, mapid);
    }

    private ArrayList<Integer> findMerge(String merTitle, ArrayList<Integer> merge, HashMap<Integer, ArrayList<ArrayList<Integer>>> degMap) {
        ArrayList<Integer> calmerge = new ArrayList<>();
        if (updatedDegree.get(merTitle).keySet().contains(merge)) {
            calmerge = merge;
            mergeindexcount = 0;
            for (Integer arr : calmerge) {
                mergeindexcount += arr;
            }

            if (!degMap.containsKey(mergeindexcount) || !degMap.get(mergeindexcount).contains(calmerge)) {
                calmerge = mergeMapCandidate(degMap, merge);
            }
        } else {
            //         System.out.println("out1");
            calmerge = mergeMapCandidate(degMap, merge);
        }
        return calmerge;
    }

    private ArrayList<ArrayList<Integer>> findRef(HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> referenceMap, ArrayList<ArrayList<Integer>> refer) {
        ArrayList<ArrayList<Integer>> calDeg = new ArrayList<>();
        if (entry.getValue().keySet().contains(refer)) {
            calDeg = refer;
            indexcount = 0;
            for (ArrayList<Integer> arr : calDeg) {
                for (int i : arr) {
                    indexcount += i;
                }
            }
            if (!referenceMap.containsKey(indexcount) || !referenceMap.get(indexcount).contains(calDeg)) {
                //       System.out.println("tttt");
                calDeg = normMapCandidate(referenceMap, refer);
            }
        } else {

            //   System.out.println("out");
            calDeg = normMapCandidate(referenceMap, refer);
        }
        return calDeg;

    }

}
