package dsapara.paraComputation;

import db.structs.ComKey;
import dscaler.dataStruct.CoDa;

import dsapara.Sort;
import dscaler.dataStruct.AvaliableStatistics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author workshop
 */
public class ParaRVCorr implements Runnable {

    HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats;
    HashMap<ArrayList<ArrayList<Integer>>, Integer> originalRVDistribution;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJointDegreeDistribution;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDisMap;
    HashSet<ArrayList<ArrayList<Integer>>> overBoundedRVs = new HashSet<>();
    HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap = new HashMap<>();
    HashMap<ArrayList<ArrayList<Integer>>, Integer> previouslyCalculatedBound = new HashMap<>();
    boolean terminated = false;
    public double sRatio;
   // double sE = 0;

    public String curTable;
    public HashMap<String, ArrayList<ComKey>> referencingComKeyMap;
    int budget = 0;
    int iteration = 0;
    int indexcount = 0;
    long starterRandom = 0;
    int thresh = 5;

    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMapping;
    public HashMap<String, Boolean> uniqueNess;
    public CoDa originalCoDa;
    ArrayList<Integer> jointDegreeAvaIndexes = new ArrayList<>();
    ArrayList<String> referencedTables = new ArrayList<>();

    public ParaRVCorr(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDisMap,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> originalRVDis,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle) {
        this.jointDegreeAvaStats = jointDegreeAvaStats;
        this.originalRVDistribution = originalRVDis;
        this.scaledJointDegreeDistribution = scaledJDDis;
        this.mergedDegreeTitle = mergedDegreeTitle;
        this.scaledRVDisMap = scaledRVDisMap;
        this.jdSumMap = jdSumMap;
    }

    @Override
    public void run() {
        calculateJDAvaIndexes();

        scaledRVDisMap.put(this.curTable, rvCorrelation());
        terminated = true;

    }
    
    /**
     * RV Correlation
     * @return 
     */
    private HashMap<ArrayList<ArrayList<Integer>>, Integer> rvCorrelation() {
        HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution = new HashMap<>();

        ArrayList<ArrayList<ComKey>> comkeys = calculateCKsOfReferencedTable();

        List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortedRVDistribution = sortRVBasedOnJDAppearance(comkeys);

        rvCorrelationOneLoop(scaledRVDistribution, sortedRVDistribution);

        while (iteration < 4 && checkAvailableJD()) {
            rvCorrelationOneLoop(scaledRVDistribution, sortedRVDistribution);
        }

        iteration = 5;

        if (checkAvailableJD()) {
            randomRoundEffi(scaledRVDistribution);
        }

        if (checkAvailableJD()) {
            randomSwapEffi(scaledRVDistribution);
        }

        return scaledRVDistribution;
    }
    
    /**
     * This method forms two new RVs
     * @param pair1
     * @param pair2
     * @param rv
     * @param originalRV
     * @param replacingIndex
     * @return 
     */
    private void formTwoNewRVs(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2,
            ArrayList<ArrayList<Integer>> rv, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV, int replacingIndex) {

        for (int i = 0; i < replacingIndex; i++) {
            pair1.add(rv.get(i));
            pair2.add(originalRV.getKey().get(i));
        }
        pair2.add(rv.get(replacingIndex));
        pair1.add(originalRV.getKey().get(replacingIndex));

        for (int i = replacingIndex + 1; i < rv.size(); i++) {
            pair1.add(rv.get(i));
            pair2.add(originalRV.getKey().get(i));
        }
        return ;
    }
    
    /**
     * This method calculates the maximal allowed incremental frequency for reforming RVs.
     * @param pair1
     * @param pair2
     * @param concurrentScaledRVDis
     * @param bound1
     * @param bound2
     * @param originalRV
     * @param incrementalFrequency
     * @return 
     */
    private int calculateLegalIncrementalFrequency(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2,
            ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> concurrentScaledRVDis,
            int bound1, int bound2, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV, int incrementalFrequency) {

        int originalRVFrequency = concurrentScaledRVDis.get(originalRV.getKey());
        
        if (!concurrentScaledRVDis.containsKey(pair1)) {
            concurrentScaledRVDis.put(pair1, 0);
        }
        if (!concurrentScaledRVDis.containsKey(pair2)) {
            concurrentScaledRVDis.put(pair2, 0);
        }
        
        int legalIncrementalFrequency = Math.min(bound1 - concurrentScaledRVDis.get(pair1), bound2 - concurrentScaledRVDis.get(pair2));
        legalIncrementalFrequency = Math.min(legalIncrementalFrequency, originalRVFrequency);
        legalIncrementalFrequency = Math.min(legalIncrementalFrequency, incrementalFrequency);
        return legalIncrementalFrequency;
    }

    /**
     * Statistics update for RVs reforming
     * @param rv
     * @param pair1
     * @param pair2
     * @param legalIncrementalFrequency
     * @param incrementalFrequency
     * @param bound1
     * @param bound2
     * @param concurrentScaledRVDis
     * @return 
     */
    private int updateReformingStatistics(ArrayList<ArrayList<Integer>> rv, ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, 
            int legalIncrementalFrequency, int incrementalFrequency, int bound1, int bound2, 
            ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> concurrentScaledRVDis) {
        concurrentScaledRVDis.put(pair1, legalIncrementalFrequency + concurrentScaledRVDis.get(pair1));
        concurrentScaledRVDis.put(pair2, legalIncrementalFrequency + concurrentScaledRVDis.get(pair2));

        incrementalFrequency = incrementalFrequency - legalIncrementalFrequency;
        updateStatisticsForRandom(rv, legalIncrementalFrequency);
        
        return incrementalFrequency;
    }
    
    
    /**
     * Forming the RVs by looping the originalRVDistribution
     * @param scaledRVDistribution
     * @param sortedRVDistribution 
     */
    private void rvCorrelationOneLoop(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution,
            List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortedRVDistribution) {
        iteration++;
        for (int i = 0; i < sortedRVDistribution.size(); i++) {

            int frequency = this.originalCoDa.rvDistribution.get(this.curTable).get(sortedRVDistribution.get(i).getKey());

            if (frequency == 0) {
                continue;
            }

            int incrementalFrequency = calExpectedFrequency(frequency);

            if (incrementalFrequency == 0) {
                continue;
            }

            if (iteration == 1) {
                perfectRVCorrelationOneStep(sortedRVDistribution.get(i).getKey(), scaledRVDistribution, incrementalFrequency);
            }
            if (iteration != 1) {
                normalRVCorrelationOneStep(sortedRVDistribution.get(i).getKey(), scaledRVDistribution, incrementalFrequency);
            }
        }

    }
    
     /**
     * This synthesis use the original RV
     * @param originalRV
     * @param scaledRVDistribution
     * @param incrementalFrequency 
     */
    private void perfectRVCorrelationOneStep(ArrayList<ArrayList<Integer>> originalRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, int incrementalFrequency) {

        if (!checkPerfectMatching(originalRV)) {
            return;
        }

        synthesizeRV(originalRV, incrementalFrequency, originalRV, scaledRVDistribution);
        return;
    }
    
    /**
     * Different from perfectRVCorrelationOneStep,
     * This method forms the RV normally.
     * @param originalRV
     * @param scaledRVDistribution
     * @param incrementalFrequency 
     */
    private void normalRVCorrelationOneStep(ArrayList<ArrayList<Integer>> originalRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, int incrementalFrequency) {

        ArrayList<ArrayList<ArrayList<Integer>>> rvSet = new ArrayList<>();
        int totalPermutationNumber = calculateRVSet(originalRV, rvSet);

        budget = incrementalFrequency;
        this.starterRandom = -1;
        while (starterRandom < totalPermutationNumber - 1 && budget > 0) {
            starterRandom++;

            ArrayList<ArrayList<Integer>> calculatedRV = fetchSingleRV(totalPermutationNumber, rvSet, originalRV);

            if (calculatedRV.size() == 0) {
                continue;
            }
            if (this.starterRandom >= totalPermutationNumber) {
                break;
            }

            if (!checkPerfectMatching(calculatedRV)) {
                continue;
            }

            synthesizeRV(calculatedRV, incrementalFrequency, originalRV, scaledRVDistribution);

        }

        return;
    }
    
    /**
     * Calculates the expected frequency
     * @param frequency
     * @return expected frequency 
     */
    private int calExpectedFrequency(Integer frequency) {
        int result = (int) (frequency * sRatio );
        double difff = frequency * sRatio - result;
        if (Math.random() < difff) {
            result++;
        }
        return result;
    }
    
   
    
    /**
     * Updates the statistics
     * @param calculatedRV
     * @param scaledRVDistribution
     * @param incrementalFrequency
     * @param originalRV 
     */
    private void updateStatistics(ArrayList<ArrayList<Integer>> calculatedRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, int incrementalFrequency,
            ArrayList<ArrayList<Integer>> originalRV) {

        int oldFrequency = 0;
        if (scaledRVDistribution.containsKey(calculatedRV)) {
            oldFrequency = scaledRVDistribution.get(calculatedRV);
        }

        if (this.uniqueNess.get(curTable)) {
            incrementalFrequency = calculateIncrementalFrequencyByUniqueness(calculatedRV, incrementalFrequency, oldFrequency);
        }

        if (incrementalFrequency <= 0) {
            return;
        }

        scaledRVDistribution.put(calculatedRV, incrementalFrequency + oldFrequency);

        updateJointDegreeAvaStats(calculatedRV, incrementalFrequency);

        budget = budget - incrementalFrequency;

        cleanNorm1AndDistanceMap(calculatedRV, originalRV);

    }
    
   /**
    * 
    * @param calculatedRV
    * @return maximal allowed frequency
    */
    private int calculateFrequencyUpperBound(ArrayList<ArrayList<Integer>> calculatedRV) {
        int bound = 1;

        if (previouslyCalculatedBound.containsKey(calculatedRV)) {
            bound = this.previouslyCalculatedBound.get(calculatedRV);
        } else if (calculatedRV.size() == 1) {
            bound = Integer.MAX_VALUE;
        } else {
            for (int t = 0; t < calculatedRV.size(); t++) {
                String sourceTable = this.referencingComKeyMap.get(curTable).get(t).sourceTable;
                if (bound >= Integer.MAX_VALUE / scaledJointDegreeDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(calculatedRV.get(t))) {
                    bound = Integer.MAX_VALUE;
                    break;
                } else {
                    bound = bound * scaledJointDegreeDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(calculatedRV.get(t));
                }
            }
            previouslyCalculatedBound.put(calculatedRV, bound);
        }

        return bound;
    }

    /**
     *
     * @param originalRV
     * @return if the scaled joint-degree distribution contains the calculated
     * referencing vector
     */
    private boolean checkPerfectMatching(ArrayList<ArrayList<Integer>> originalRV) {
        for (int i = 0; i < originalRV.size(); i++) {
            if (!jointDegreeAvaStats.get(referencingComKeyMap.get(curTable).get(i).getSourceTable()).containsKey(originalRV.get(i))) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Synthesize new RVs
     * @param calculatedRV
     * @param incrementalFrequency
     * @param originalRV
     * @param scaledRVDistribution 
     */
    private void synthesizeRV(ArrayList<ArrayList<Integer>> calculatedRV, int incrementalFrequency,
            ArrayList<ArrayList<Integer>> originalRV, HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution) {

        incrementalFrequency = calculateAvaliableFrequency(calculatedRV, incrementalFrequency);

        if (!this.uniqueNess.get(curTable) || !this.overBoundedRVs.contains(calculatedRV)) {
            updateStatistics(calculatedRV, scaledRVDistribution, incrementalFrequency, originalRV);
        }
    }
    
  

    /**
     *
     * @param incrementalFrequency
     * @param rv
     * @param concurrentScaledRVDis
     * @param uniquenessBound
     * @return newly calculated incrementalFrequency
     */
    private int synthesizeNewRV(int incrementalFrequency, ArrayList<ArrayList<Integer>> rv,
            ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> concurrentScaledRVDis,
            long uniquenessBound) {
        int oldFrequency = 0;
        if (concurrentScaledRVDis.containsKey(rv) && concurrentScaledRVDis.get(rv) < uniquenessBound) {
            oldFrequency = concurrentScaledRVDis.get(rv);
        }
        incrementalFrequency = (int) Math.min(incrementalFrequency, uniquenessBound - oldFrequency);
        concurrentScaledRVDis.put(rv, oldFrequency + incrementalFrequency);

        if (concurrentScaledRVDis.get(rv) == this.previouslyCalculatedBound.get(rv)) {
            this.previouslyCalculatedBound.remove(rv);
            overBoundedRVs.add(rv);
        }

        return incrementalFrequency;
    }

    
    /**
     * Calculate the prodution of the array,
     * For enumerating RV purpose
     * @param rvSet
     * @param i
     * @return 
     */
    private int calIndexSize(ArrayList<ArrayList<ArrayList<Integer>>> rvSet, int i) {
        int size = 1;
        for (int k = i + 1; k < rvSet.size(); k++) {
            size = size * rvSet.get(k).size();
        }
        return size;
    }
    
    
    /**
     * This method breaks the oldRV, candidateRV and
     * forms two new RVs
     * 
     * @param rv
     * @param originalRV
     * @param incrementalFrequency
     * @param concurrentScaledRVDis
     * @return leftOverFrequency
     */
    private int breakAndFormPairs(ArrayList<ArrayList<Integer>> rv, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV,
            int incrementalFrequency, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> concurrentScaledRVDis) {
        for (int replacingIndex = 0; replacingIndex < rv.size(); replacingIndex++) {
            ArrayList<ArrayList<Integer>> pair1 = new ArrayList<>();
            ArrayList<ArrayList<Integer>> pair2 = new ArrayList<>();

            formTwoNewRVs(pair1, pair2, rv, originalRV, replacingIndex);

            int bound1 = Integer.MAX_VALUE;
            int bound2 = Integer.MAX_VALUE;

            if (uniqueNess.get(curTable)) {
                bound1 = calculateFrequencyUpperBound(pair1);
                bound2 = calculateFrequencyUpperBound(pair2);
            }

            if ((!concurrentScaledRVDis.containsKey(pair1) || concurrentScaledRVDis.get(pair1) < bound1)
                    && (!concurrentScaledRVDis.containsKey(pair2) || concurrentScaledRVDis.get(pair2) < bound2)) {

                int legalIncrementalFrequency = calculateLegalIncrementalFrequency(pair1, pair2, concurrentScaledRVDis, bound1, bound2, originalRV, incrementalFrequency);
                if (legalIncrementalFrequency <= 0) {
                    continue;
                }
                concurrentScaledRVDis.put(originalRV.getKey(), originalRV.getValue() - legalIncrementalFrequency);
                incrementalFrequency = updateReformingStatistics(rv, pair1, pair2, legalIncrementalFrequency, incrementalFrequency, bound1, bound2, concurrentScaledRVDis);

                break;

            }
        }

        return incrementalFrequency;
    }

    /**
     * This method retrieves the closest joint-degrees based on the key sum.
     *
     * @param originalJD
     * @param ck
     * @return
     */
    private ArrayList<ArrayList<Integer>> calculateClosestJointDegree(ArrayList<Integer> originalJD, ComKey ck) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        int thresh = 10000;
        int sum = 0;
        for (int i : originalJD) {
            sum += i;
        }
        for (int i = 1; i <= thresh; i++) {
            for (int k = -1; k <= 1; k = k + 2) {
                if (this.jdSumMap.get(ck).containsKey(sum + k * i)) {
                    return jdSumMap.get(ck).get(sum + k * i);
                }
            }
            if (min <= i) {
                return result;
            }
        }

        return result;

    }

    /**
     * This method retrieves the best matching RV By iterating through the
     * possible candidates
     *
     * @param totalNum
     * @param rvSet
     * @param originalRV
     * @return bestMatchingRV
     */
    private ArrayList<ArrayList<Integer>> fetchSingleRV(int totalNum, ArrayList<ArrayList<ArrayList<Integer>>> rvSet,
            ArrayList<ArrayList<Integer>> originalRV) {

        ArrayList<ArrayList<Integer>> matchingRV = new ArrayList<>();
        while (starterRandom < totalNum) {
            matchingRV = new ArrayList<>();
            long remainder = this.starterRandom;
            for (int i = 0; i < rvSet.size(); i++) {
                int indexSize = calIndexSize(rvSet, i);
                int ind = (int) (remainder / indexSize);
                String srcTable = referencedTables.get(i);
                int avaIndex = jointDegreeAvaIndexes.get(i);

                if (jointDegreeAvaStats.get(srcTable).containsKey(rvSet.get(i).get(ind))
                        && jointDegreeAvaStats.get(srcTable).get(rvSet.get(i).get(ind)).ckAvaCount[avaIndex] > 0) {
                    matchingRV.add(rvSet.get(i).get(ind));
                    remainder = remainder % indexSize;
                } else {
                    //clean the hashmaps
                    this.starterRandom++;
                    if (norm1JointDegreeMapping.get(this.mergedDegreeTitle.get(srcTable)).containsKey(originalRV.get(i))) {
                        norm1JointDegreeMapping.get(this.mergedDegreeTitle.get(srcTable)).get(originalRV.get(i)).remove(rvSet.get(i).get(ind));
                    }
                    cleanDistanceMap(i, rvSet.get(i).get(ind));
                    break;
                }
            }
            if (matchingRV.size() == rvSet.size()) {
                return matchingRV;
            }
        }
        return matchingRV;
    }

    /**
     *
     * @return CKsOfErerencedTables
     */
    private ArrayList<ArrayList<ComKey>> calculateCKsOfReferencedTable() {
        ArrayList<ArrayList<ComKey>> comKeys = new ArrayList<>();
        for (int i = 0; i < referencingComKeyMap.get(curTable).size(); i++) {
            ComKey ck = referencingComKeyMap.get(curTable).get(i);
            String srcTable = ck.getSourceTable();
            comKeys.add(this.mergedDegreeTitle.get(srcTable));
        }
        return comKeys;
    }

    
    /**
     *
     * @param comkeys
     * @return sortedRVList
     */
    private List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortRVBasedOnJDAppearance(ArrayList<ArrayList<ComKey>> comkeys) {
        HashMap<ArrayList<ArrayList<Integer>>, Long> rvAppearance = new HashMap<>();
        for (ArrayList<ArrayList<Integer>> rv : originalCoDa.rvDistribution.get(curTable).keySet()) {
            long product = 1;
            for (int i = 0; i < rv.size(); i++) {
                product *= originalCoDa.jointDegreeDis.get(comkeys.get(i)).get(rv.get(i));
            }
            rvAppearance.put(rv, product);
        }

        return new Sort().sortOnKeyAppearance(rvAppearance, comkeys, referencingComKeyMap, originalCoDa.jointDegreeDis);
    }

 

    /**
     * This method calculates the candidate RVs
     *
     * @param originalRV
     * @param rvSet
     * @return productNumber of the candidate RVs
     */
    private int calculateRVSet(ArrayList<ArrayList<Integer>> originalRV, ArrayList<ArrayList<ArrayList<Integer>>> rvSet) {
        int productNumber = 1;
        for (int i = 0; i < referencingComKeyMap.get(curTable).size(); i++) {
            String sourceTable = referencingComKeyMap.get(curTable).get(i).sourceTable;

            ArrayList<ArrayList<Integer>> cloesestJDs = new ArrayList<ArrayList<Integer>>();
            if (norm1JointDegreeMapping.get(mergedDegreeTitle.get(sourceTable)).containsKey(originalRV.get(i))) {
                for (ArrayList<Integer> jointDegree : norm1JointDegreeMapping.get(mergedDegreeTitle.get(sourceTable)).get(originalRV.get(i))) {
                    cloesestJDs.add(new ArrayList<Integer>(jointDegree));
                }
            } else if (cloesestJDs.size() == 0) {
                cloesestJDs = new ArrayList<>(calculateClosestJointDegree(originalRV.get(i), referencingComKeyMap.get(curTable).get(i)));
            }

            if (cloesestJDs.size() == 0) {
                return -1;
            }

            rvSet.add(cloesestJDs);
            productNumber *= cloesestJDs.size();
        }
        return productNumber;
    }


    /**
     * Update the avaStas for incrementalFrequency
     *
     * @param rv
     * @param incrementalFrequency
     */
    private void updateStatisticsForRandom(ArrayList<ArrayList<Integer>> rv, int incrementalFrequency) {
        for (int i = 0; i < rv.size(); i++) {
            String srcTable = referencedTables.get(i);
            int index = jointDegreeAvaIndexes.get(i);
            jointDegreeAvaStats.get(srcTable).get(rv.get(i)).ckAvaCount[index] -= incrementalFrequency;
        }
    }
    
    
    /**
     * Randomly pairs up the JDs to form new RVs
     * @param scaledRVDistribution 
     */
    private void randomRoundEffi(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution
           ) {
 ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees = calculateAvailableJD();

        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> concurrentScaledRVDis = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : scaledRVDistribution.entrySet()) {
            concurrentScaledRVDis.put(entry.getKey(), entry.getValue());
        }

        long uniquenessBound = Integer.MAX_VALUE;
        starterRandom = 0;
        ArrayList<Integer> jointDegreeIndexs = new ArrayList<>(referencedTables.size());
        for (int i = 0; i < referencedTables.size(); i++) {
            jointDegreeIndexs.add(0);
        }

        while (checkIndexesOverflow(jointDegreeIndexs, availableJointDegrees)) {
            System.out.println("indexes:" + jointDegreeIndexs);
            int incrementalFrequency = calculateAvailableFrequency(jointDegreeIndexs, availableJointDegrees);

            if (incrementalFrequency == 0) {
                continue;
            }
            if (incrementalFrequency < 0) {
                break;
            }

            uniquenessBound = calculateUniquenessBound(jointDegreeIndexs, availableJointDegrees);
            ArrayList<ArrayList<Integer>> rv = extractRV(jointDegreeIndexs, availableJointDegrees);

            incrementalFrequency = synthesizeNewRV(incrementalFrequency, rv, concurrentScaledRVDis, uniquenessBound);
            updateStatisticsForRandom(rv, incrementalFrequency);

            if (incrementJointDegreeIndexes(jointDegreeIndexs, jointDegreeIndexs.size() - 1, availableJointDegrees) < 0) {
                break;
            }
        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : concurrentScaledRVDis.entrySet()) {
            scaledRVDistribution.put(entry.getKey(), entry.getValue());
        }
    }
    
    
    /**
     * Randomly swaps the old RV and a potential RV 
     * to form 2 more RVs
     * @param scaledRVDistribution 
     */
    private void randomSwapEffi(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution) {
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> concurrentScaledRVDis = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : scaledRVDistribution.entrySet()) {
            concurrentScaledRVDis.put(entry.getKey(), entry.getValue());
        }

        ArrayList<Integer> jointDegreeIndexs = new ArrayList<>(referencedTables.size());
        for (int i = 0; i < referencedTables.size(); i++) {
            jointDegreeIndexs.add(0);
        }

        ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees = calculateAvailableJD();

        starterRandom = 0;
        while (checkIndexesOverflow(jointDegreeIndexs, availableJointDegrees)) {
            int incrementalFrequency = calculateAvailableFrequency(jointDegreeIndexs, availableJointDegrees);
            if (incrementalFrequency == 0) {
                continue;
            }
            if (incrementalFrequency < 0) {
                break;
            }

            ArrayList<ArrayList<Integer>> rv = extractRV(jointDegreeIndexs, availableJointDegrees);

            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV : concurrentScaledRVDis.entrySet()) {
                if (originalRV.getValue() == 0 || originalRV.getKey().size() == 0) {
                    continue;
                }

                incrementalFrequency = breakAndFormPairs(rv, originalRV, incrementalFrequency, concurrentScaledRVDis);

                if (incrementalFrequency == 0) {
                    continue;
                }

            }

            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : concurrentScaledRVDis.entrySet()) {
                scaledRVDistribution.put(entry.getKey(), entry.getValue());
            }
        }
    }

    

    /**
     * Returns FALSE if it has enumerated all the cases.
     *
     * @param jointDegreeIndexs
     * @param availableJointDegrees
     * @return if the indexes are still legal
     */
    private boolean checkIndexesOverflow(ArrayList<Integer> jointDegreeIndexs, ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees) {

        for (int i = 0; i < jointDegreeIndexs.size(); i++) {
            if (jointDegreeIndexs.get(i) < availableJointDegrees.get(i).size()) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method determines if the corresponding RV is legal 
     * and returns the corresponding frequency
     * If the returned value is below 0, 
     * Then no more RV are available.
     * @param jointDegreeIndexes
     * @param availableJointDegrees
     * @return 
     */
    private int calculateAvailableFrequency(ArrayList<Integer> jointDegreeIndexes, ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees) {
        int minNumber = Integer.MAX_VALUE;
        for (int i = 0; i < jointDegreeIndexes.size(); i++) {
            ArrayList<Integer> jointDegree = availableJointDegrees.get(i).get(jointDegreeIndexes.get(i));
            String srcTable = referencedTables.get(i);
            int jdIndex = jointDegreeIndexes.get(i);
            if (jointDegreeAvaStats.get(srcTable).containsKey(jointDegree) && jointDegreeAvaStats.get(srcTable).get(jointDegree).ckAvaCount[jdIndex] > 0) {
                minNumber = Math.min(minNumber, jointDegreeAvaStats.get(srcTable).get(jointDegree).ckAvaCount[jdIndex]);
            } else {
                int returnNumber = incrementJointDegreeIndexes(jointDegreeIndexes, i, availableJointDegrees);
                return returnNumber;
            }
        }

        return minNumber;
    }

    /**
     * Increment the joinDegreeIndexes
     *
     * @param jointDegreeIndexes
     * @param i
     * @param availableJointDegrees
     * @return
     */
    private int incrementJointDegreeIndexes(ArrayList<Integer> jointDegreeIndexes, int i, ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees) {
        if (i < 0) {
            return 0;
        }

        for (int j = i + 1; j < jointDegreeIndexes.size(); j++) {
            jointDegreeIndexes.set(j, 0);
        }

        if (i == 0 && jointDegreeIndexes.get(i) >= availableJointDegrees.get(i).size() - 1) {
            return -1;
        } else if (jointDegreeIndexes.get(i) == availableJointDegrees.get(i).size() - 1) {
            return incrementJointDegreeIndexes(jointDegreeIndexes, i - 1, availableJointDegrees);
        } else {
            jointDegreeIndexes.set(i, (jointDegreeIndexes.get(i) + 1));
        }
        return 0;
    }

    
    
    /**
     * 
     * @param jointDegreeIndexs
     * @param availableJointDegrees
     * @return bound by uniqueness constraint
     */
    private long calculateUniquenessBound(ArrayList<Integer> jointDegreeIndexs,
            ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees) {
        if (this.uniqueNess.get(curTable)) {
            long result = 1;
            for (int i = 0; i < referencedTables.size(); i++) {
                long v1 = scaledJointDegreeDistribution.get(this.mergedDegreeTitle.get(referencedTables.get(i))).get(availableJointDegrees.get(i).get(jointDegreeIndexs.get(i)));
                result = result * v1;
            }

            return result;
        }

        return Long.MAX_VALUE;
    }
    
    
    /**
     * Extract the RV based on the jointdegreeIndexes
     * @param jointDegreeIndexs
     * @param availableJointDegrees
     * @return RV
     */
    private ArrayList<ArrayList<Integer>> extractRV(ArrayList<Integer> jointDegreeIndexs,
            ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i < jointDegreeIndexs.size(); i++) {
            result.add(availableJointDegrees.get(i).get(jointDegreeIndexs.get(i)));
        }
        return result;
    }
    
    
    /**
     * Initialize the parameters
     * @param originalCoDa
     * @param sRatio
     * @param curTable
     * @param referencingTableMap
     * @param norm1JointDegreeMapping
     * @param uniqueNess 
     */
    public void setInitials(CoDa originalCoDa, double sRatio, String curTable,
            HashMap<String, ArrayList<ComKey>> referencingTableMap,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMapping,
            HashMap<String, Boolean> uniqueNess) {
        this.originalCoDa = originalCoDa;
        this.sRatio = sRatio;
        this.curTable = curTable;
        this.referencingComKeyMap = referencingTableMap;
        this.norm1JointDegreeMapping = norm1JointDegreeMapping;
        this.uniqueNess = uniqueNess;
    }   

    
    /**
     * Preparation Work: Calculate the indexes for each referenced JD.
     */
    private void calculateJDAvaIndexes() {
        for (ComKey ck : referencingComKeyMap.get(curTable)) {
            String srcTable = ck.getSourceTable();
            referencedTables.add(srcTable);
            for (int i = 0; i < mergedDegreeTitle.get(srcTable).size(); i++) {
                if (mergedDegreeTitle.get(srcTable).get(i).getReferencingTable().equals(curTable) && ck.referenceposition == mergedDegreeTitle.get(srcTable).get(i).referenceposition) {
                    jointDegreeAvaIndexes.add(i);
                }
            }
        }
    }

    /**
     *
     * @param calculatedRV
     * @param frequency
     * @return maximal avaliable frequencies
     */
    private int calculateAvaliableFrequency(ArrayList<ArrayList<Integer>> calculatedRV, int frequency) {
        for (int i = 0; i < calculatedRV.size(); i++) {
            String srcTable = referencedTables.get(i);
            int index = jointDegreeAvaIndexes.get(i);
            frequency = Math.min(frequency, jointDegreeAvaStats.get(srcTable).get(calculatedRV.get(i)).ckAvaCount[index]);
        }
        return frequency;
    }
    
    
    /**
     * Calculate the incremental frequency for uniqueness constraint
     * @param calculatedRV
     * @param incrementalFrequency
     * @param oldFrequency
     * @return incrementalFrequency
     */
    private int calculateIncrementalFrequencyByUniqueness(ArrayList<ArrayList<Integer>> calculatedRV, int incrementalFrequency, int oldFrequency) {
        int bound = calculateFrequencyUpperBound(calculatedRV);

        if (oldFrequency + incrementalFrequency >= bound) {
            this.previouslyCalculatedBound.remove(calculatedRV);
            this.overBoundedRVs.add(calculatedRV);
            incrementalFrequency = bound - oldFrequency;
        }
        return incrementalFrequency;
    }
    
    
    /**
     * Update the joint-degree available statistics
     * @param calculatedRV
     * @param incrementalFrequency 
     */
    private void updateJointDegreeAvaStats(ArrayList<ArrayList<Integer>> calculatedRV, int incrementalFrequency) {
        for (int k = 0; k < calculatedRV.size(); k++) {
            String srcTable = referencedTables.get(k);
            int index = jointDegreeAvaIndexes.get(k);
            int v = jointDegreeAvaStats.get(srcTable).get(calculatedRV.get(k)).ckAvaCount[index];
            jointDegreeAvaStats.get(srcTable).get(calculatedRV.get(k)).ckAvaCount[index] = v - incrementalFrequency;
        }
    }

    /**
     * Cleaning Job
     *
     * @param calculatedRV
     * @param originalRV
     */
    private void cleanNorm1AndDistanceMap(ArrayList<ArrayList<Integer>> calculatedRV, ArrayList<ArrayList<Integer>> originalRV) {
        for (int k = 0; k < calculatedRV.size(); k++) {
            String sourceTable = this.referencedTables.get(k);
            int index = jointDegreeAvaIndexes.get(k);
            int v = jointDegreeAvaStats.get(sourceTable).get(calculatedRV.get(k)).ckAvaCount[index];
            if (v == 0) {

                cleanNorm1Map(sourceTable, originalRV, calculatedRV, k);

                cleanDistanceMap(k, calculatedRV.get(k));
            }
        }
    }

    /**
     * Cleans the norm1 map
     *
     * @param sourceTable
     * @param originalRV
     * @param calculatedRV
     * @param k
     */
    private void cleanNorm1Map(String sourceTable, ArrayList<ArrayList<Integer>> originalRV, ArrayList<ArrayList<Integer>> calculatedRV, int k) {
        if (this.norm1JointDegreeMapping.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(originalRV.get(k))) {
            if (this.norm1JointDegreeMapping.get(this.mergedDegreeTitle.get(sourceTable)).get(originalRV.get(k)).contains(calculatedRV.get(k))) {
                this.norm1JointDegreeMapping.get(this.mergedDegreeTitle.get(sourceTable)).get(originalRV.get(k)).remove(calculatedRV.get(k));
            }
        }
    }

    /**
     * Cleans the jdSumMap
     *
     * @param k
     * @param calDegrees
     */
    private void cleanDistanceMap(int k, ArrayList<Integer> calDegrees) {
        int insum = calculateDegreeSum(calDegrees);
        ComKey ck = referencingComKeyMap.get(curTable).get(k);
        if (jdSumMap.containsKey(ck) && jdSumMap.get(ck).containsKey(insum)) {
            this.jdSumMap.get(referencingComKeyMap.get(curTable).get(k)).get(insum).remove(calDegrees);
            if (this.jdSumMap.get(referencingComKeyMap.get(curTable).get(k)).get(insum).isEmpty()) {
                this.jdSumMap.get(referencingComKeyMap.get(curTable).get(k)).remove(insum);
            }
        }

    }

    /**
     *
     * @return avaliable Joint-Degrees
     */
    private ArrayList<ArrayList<ArrayList<Integer>>> calculateAvailableJD() {
        ArrayList<ArrayList<ArrayList<Integer>>> result = new ArrayList<>();
        for (int i = 0; i < referencedTables.size(); i++) {
            ArrayList<ArrayList<Integer>> jointDegrees = new ArrayList<>();
            String srcTable = referencedTables.get(i);
            int index = jointDegreeAvaIndexes.get(i);
            for (Entry<ArrayList<Integer>, AvaliableStatistics> entry : jointDegreeAvaStats.get(srcTable).entrySet()) {
                if (entry.getValue().ckAvaCount[index] > 0) {
                    jointDegrees.add(entry.getKey());
                }
            }
            result.add(jointDegrees);
        }
        return result;
    }

    /**
     *
     * @return if there are available joint-degrees
     */
    private boolean checkAvailableJD() {
        for (int i = 0; i < referencedTables.size(); i++) {
            String srcTable = referencedTables.get(i);
            int index = jointDegreeAvaIndexes.get(i);
            for (Entry<ArrayList<Integer>, AvaliableStatistics> entry : jointDegreeAvaStats.get(srcTable).entrySet()) {
                if (entry.getValue().ckAvaCount[index] > 0) {
                    return true;
                }
            }

        }
        return false;
    }

    
    /**
     * 
     * @param jointDegree
     * @return sum of the degrees
     */
    private int calculateDegreeSum(ArrayList<Integer> jointDegree) {
        int sum = 0;
        for (int num: jointDegree){
            sum += num;
        }
        return sum;
    }

}
