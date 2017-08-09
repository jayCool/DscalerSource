package paraComputation;

import com.sun.corba.se.impl.io.ValueHandlerImpl;
import db.structs.ComKey;
import dataStructure.CoDa;

import main.Sort;
import dataStructure.AvaliableStatistics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Zhang Jiangwei
 */
public class ParaRVCorr implements Runnable {

    HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJointDegreeDistribution;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDisMap;
    HashSet<ArrayList<ArrayList<Integer>>> overBoundedRVs = new HashSet<>();
    HashMap<ArrayList<ArrayList<Integer>>, Integer> previouslyCalculatedBound = new HashMap<>();
    boolean terminated = false;
    public double sRatio;

    public String curTable;
    public HashMap<String, ArrayList<ComKey>> referencingComKeyMap;
    int budget = 0;
    int iteration = 0;
    int indexcount = 0;
    long starterRandom = 0;
    int thresh = 5;

    public ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList;
    public HashMap<String, Boolean> uniqueNess;
    public CoDa originalCoDa;
    ArrayList<Integer> jointDegreeAvaIndexes = new ArrayList<>();
    ArrayList<String> referencedTables = new ArrayList<>();

    int debugTotal = 0;
    ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> originalJDSumMapping = new ArrayList<>();
    ArrayList<Integer>[] currentlyUsingOriginalJDs;
    boolean kvTable = false;
    HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>> norm1RVMapping = new HashMap<>();

    public ParaRVCorr(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDisMap,
            HashMap<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jointDegreeAvaStats,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> originalRVDis,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle) {
        this.jointDegreeAvaStats = jointDegreeAvaStats;
        this.scaledJointDegreeDistribution = scaledJDDis;
        this.mergedDegreeTitle = mergedDegreeTitle;
        this.scaledRVDisMap = scaledRVDisMap;
    }

    @Override
    public void run() {
        currentlyUsingOriginalJDs = new ArrayList[this.referencingComKeyMap.get(curTable).size()];
        calculateJDAvaIndexes();

        scaledRVDisMap.put(this.curTable, rvCorrelation());
        terminated = true;

    }

    /**
     * RV Correlation
     *
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

        iteration = 4;

        if (checkAvailableJD()) {
            generateRVList();
            randomRoundEffi(scaledRVDistribution);
        }

        if (checkAvailableJD()) {
            randomSwapEffi(scaledRVDistribution);
        }

        return scaledRVDistribution;
    }

    /**
     * This method forms two new RVs
     *
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
        return;
    }

    /**
     * This method calculates the maximal allowed incremental frequency for
     * reforming RVs.
     *
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
     *
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
        ArrayList<ArrayList<Integer>> randomizedRV = rvList.get(rand.nextInt(rvList.size()));
        if (kvTable) {
            updateRVNormMap(randomizedRV, pair1);
        }
        if (kvTable) {
            updateRVNormMap(randomizedRV, pair2);
        }

        incrementalFrequency = incrementalFrequency - legalIncrementalFrequency;
        updateStatisticsForRandom(rv, legalIncrementalFrequency);

        return incrementalFrequency;
    }

    /**
     * Forming the RVs by looping the originalRVDistribution
     *
     * @param scaledRVDistribution
     * @param sortedRVDistribution
     */
    private void rvCorrelationOneLoop(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution,
            List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortedRVDistribution) {
        iteration++;
        int tempTotal = 0;

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
                tempTotal += incrementalFrequency;
                normalRVCorrelationOneStep(sortedRVDistribution.get(i).getKey(), scaledRVDistribution, incrementalFrequency);
            }

        }
    }

    /**
     * This synthesis use the original RV
     *
     * @param originalRV
     * @param scaledRVDistribution
     * @param incrementalFrequency
     */
    private void perfectRVCorrelationOneStep(ArrayList<ArrayList<Integer>> originalRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, int incrementalFrequency) {

        if (!checkPerfectMatching(originalRV)) {
            return;
        }
        for (int i = 0; i < originalRV.size(); i++) {
            currentlyUsingOriginalJDs[i] = originalRV.get(i);
        }
        synthesizeRV(originalRV, incrementalFrequency, currentlyUsingOriginalJDs, scaledRVDistribution, originalRV);
        return;
    }

    /**
     * Different from perfectRVCorrelationOneStep, This method forms the RV
     * normally.
     *
     * @param originalRV
     * @param scaledRVDistribution
     * @param incrementalFrequency
     */
    private void normalRVCorrelationOneStep(ArrayList<ArrayList<Integer>> originalRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, int incrementalFrequency) {

        budget = incrementalFrequency;
        this.starterRandom = -1;
        while (budget > 0) {

            budget = incrementalFrequency;
            starterRandom = -1;
            ArrayList<ArrayList<ArrayList<Integer>>> rvSet = new ArrayList<>();
            long totalPermutationNumber = calculateRVSet(originalRV, rvSet);
            if (totalPermutationNumber == -1) {
                break;
            }

            while (starterRandom < totalPermutationNumber - 1 && budget > 0) {
                starterRandom++;
                ArrayList<ArrayList<Integer>> calculatedRV = fetchSingleRV(totalPermutationNumber, rvSet, currentlyUsingOriginalJDs);
                if (calculatedRV.isEmpty()) {
                    continue;
                }
                if (this.starterRandom >= totalPermutationNumber) {
                    break;
                }

                if (!checkPerfectMatching(calculatedRV)) {
                    continue;
                }
                synthesizeRV(calculatedRV, incrementalFrequency, currentlyUsingOriginalJDs, scaledRVDistribution, originalRV);
                incrementalFrequency = budget;
            }
        }
        return;
    }

    /**
     * Calculates the expected frequency
     *
     * @param frequency
     * @return expected frequency
     */
    private int calExpectedFrequency(Integer frequency) {
        int result = (int) (frequency * sRatio);

        double diff = frequency * sRatio - result;

        if (Math.random() < diff) {
            result++;
        }
        return result;
    }

    /**
     * Updates the statistics
     *
     * @param calculatedRV
     * @param scaledRVDistribution
     * @param incrementalFrequency
     * @param originalRV
     */
    private void updateStatistics(ArrayList<ArrayList<Integer>> calculatedRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, int incrementalFrequency,
            ArrayList<Integer>[] currentlyUsingOriginalJDs, ArrayList<ArrayList<Integer>> originalRV) {

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

        debugTotal += incrementalFrequency;
        if (this.kvTable) {
            updateRVNormMap(originalRV, calculatedRV);
        }

        scaledRVDistribution.put(calculatedRV, incrementalFrequency + oldFrequency);

        updateJointDegreeAvaStats(calculatedRV, incrementalFrequency);

        budget = budget - incrementalFrequency;

        cleanNorm1AndDistanceMap(calculatedRV, currentlyUsingOriginalJDs);

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
            if (!jointDegreeAvaStats.get(referencedTables.get(i)).containsKey(originalRV.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Synthesize new RVs
     *
     * @param calculatedRV
     * @param incrementalFrequency
     * @param originalRV
     * @param scaledRVDistribution
     */
    private void synthesizeRV(ArrayList<ArrayList<Integer>> calculatedRV, int incrementalFrequency,
            ArrayList<Integer>[] currentlyUsingOriginalJDs, HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDistribution, ArrayList<ArrayList<Integer>> originalRV) {

        incrementalFrequency = calculateAvaliableFrequency(calculatedRV, incrementalFrequency, currentlyUsingOriginalJDs);

        if (!this.uniqueNess.get(curTable) || !this.overBoundedRVs.contains(calculatedRV)) {
            updateStatistics(calculatedRV, scaledRVDistribution, incrementalFrequency, currentlyUsingOriginalJDs, originalRV);
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
     * Calculate the product of the array, For enumerating RV purpose
     *
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
     * This method breaks the oldRV, candidateRV and forms two new RVs
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
     * @return closest joint-degrees
     */
    Sort sortFunction = new Sort();

    int calculateDistance(ArrayList<Integer> original, ArrayList<Integer> candidate) {
        int distance = 0;
        for (int i = 0; i < original.size(); i++) {
            distance += Math.abs(original.get(i) - candidate.get(i));
        }
        return distance;
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
    private ArrayList<ArrayList<Integer>> fetchSingleRV(long totalNum, ArrayList<ArrayList<ArrayList<Integer>>> rvSet,
            ArrayList<Integer>[] currentlyUsingOriginalJDs) {

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
                    if (norm1JointDegreeMappingList.get(i).containsKey(currentlyUsingOriginalJDs[i])) {
                        norm1JointDegreeMappingList.get(i).get(currentlyUsingOriginalJDs[i]).remove(rvSet.get(i).get(ind));
                        if (norm1JointDegreeMappingList.get(i).get(currentlyUsingOriginalJDs[i]).size() == 0) {
                            norm1JointDegreeMappingList.get(i).remove(currentlyUsingOriginalJDs[i]);
                        }
                    }
                    break;
                }
            }

            if (matchingRV.size() == rvSet.size()) {
                return matchingRV;
            } else {
                this.starterRandom++;

            }
        }
        return new ArrayList<>();
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

            for (ArrayList<ComKey> keys : originalCoDa.reverseJointDegrees.keySet()) {
                if (keys.get(0).getSourceTable().equals(srcTable)) {
                    comKeys.add(keys);
                }
            }

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
                product *= originalCoDa.reverseJointDegrees.get(comkeys.get(i)).get(rv.get(i)).size();
            }
            rvAppearance.put(rv, product);
        }

        return new Sort().sortOnKeyAppearance(rvAppearance, comkeys, referencingComKeyMap);
    }

    /**
     * This method calculates the candidate RVs
     *
     * @param originalRV
     * @param rvSet
     * @return productNumber of the candidate RVs
     */
    private long calculateRVSet(ArrayList<ArrayList<Integer>> originalRV, ArrayList<ArrayList<ArrayList<Integer>>> rvSet) {
        long productNumber = 1;
        for (int i = 0; i < referencingComKeyMap.get(curTable).size(); i++) {
            String sourceTable = referencedTables.get(i);

            ArrayList<ArrayList<Integer>> cloesestJDs = extractFromMapping(i, originalRV.get(i));
            currentlyUsingOriginalJDs[i] = originalRV.get(i);
            if (cloesestJDs.isEmpty()) {
                norm1JointDegreeMappingList.get(i).remove(originalRV.get(i));
                ArrayList<Integer> closestOrigianlJD = calculateClosestJointDegree(originalRV.get(i), i, originalJDSumMapping,
                        mergedDegreeTitle.get(sourceTable), norm1JointDegreeMappingList);
                cloesestJDs = extractFromMapping(i, closestOrigianlJD);
                currentlyUsingOriginalJDs[i] = closestOrigianlJD;
                if (cloesestJDs.isEmpty()) {
                    return -1;
                }
            }

            rvSet.add(cloesestJDs);
            if (productNumber > Integer.MAX_VALUE / cloesestJDs.size()) {
                productNumber = Integer.MAX_VALUE;
            } else {
                productNumber *= cloesestJDs.size();
            }
        }
        return productNumber;
    }

    Random rand = new Random();

    /**
     * Update the avaStas for incrementalFrequency
     *
     * @param rv
     * @param incrementalFrequency
     */
    private void updateStatisticsForRandom(ArrayList<ArrayList<Integer>> rv, int incrementalFrequency) {

        ArrayList<ArrayList<Integer>> randomizedRV = rvList.get(rand.nextInt(rvList.size()));
        if (kvTable) {
            updateRVNormMap(randomizedRV, rv);
        }
        for (int i = 0; i < rv.size(); i++) {
            String srcTable = referencedTables.get(i);
            int index = jointDegreeAvaIndexes.get(i);
            jointDegreeAvaStats.get(srcTable).get(rv.get(i)).ckAvaCount[index] -= incrementalFrequency;
        }
    }

    /**
     * Randomly pairs up the JDs to form new RVs
     *
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

        int randomeTotal = 0;
        while (checkIndexesOverflow(jointDegreeIndexs, availableJointDegrees)) {
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
            randomeTotal += incrementalFrequency;
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
     * Randomly swaps the old RV and a potential RV to form 2 more RVs
     *
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
     * This method determines if the corresponding RV is legal and returns the
     * corresponding frequency If the returned value is below 0, Then no more RV
     * are available.
     *
     * @param jointDegreeIndexes
     * @param availableJointDegrees
     * @return
     */
    private int calculateAvailableFrequency(ArrayList<Integer> jointDegreeIndexes, ArrayList<ArrayList<ArrayList<Integer>>> availableJointDegrees) {
        int minNumber = Integer.MAX_VALUE;
        for (int i = 0; i < jointDegreeIndexes.size(); i++) {
            ArrayList<Integer> jointDegree = availableJointDegrees.get(i).get(jointDegreeIndexes.get(i));
            String srcTable = referencedTables.get(i);
            int jdIndex = jointDegreeAvaIndexes.get(i);
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
     *
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
     *
     * @param originalCoDa
     * @param sRatio
     * @param curTable
     * @param referencingTableMap
     * @param norm1JointDegreeMappingList
     * @param uniqueNess
     */
    public void setInitials(boolean kvTable, CoDa originalCoDa, double sRatio, String curTable,
            HashMap<String, ArrayList<ComKey>> referencingTableMap,
            ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList,
            HashMap<String, Boolean> uniqueNess,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<ArrayList<Integer>>>>> originalRVMappedScaledRV
    ) {
        this.originalCoDa = originalCoDa;
        this.sRatio = sRatio;
        this.curTable = curTable;
        this.referencingComKeyMap = referencingTableMap;
        this.norm1JointDegreeMappingList = norm1JointDegreeMappingList;
        this.uniqueNess = uniqueNess;
        if (kvTable) {
            originalRVMappedScaledRV.put(curTable, norm1RVMapping);
        }
        this.kvTable = kvTable;
        sumValues = new int[referencingComKeyMap.get(curTable).size()][];
        removedValues = new HashSet[sumValues.length];
        for (int index = 0; index < sumValues.length; index++) {
            HashMap<Integer, ArrayList<ArrayList<Integer>>> sumMap = new HashMap<>();
            for (ArrayList<Integer> jd : norm1JointDegreeMappingList.get(index).keySet()) {
                int sum = calculateDegreeSum(jd);
                if (!sumMap.containsKey(sum)) {
                    sumMap.put(sum, new ArrayList<ArrayList<Integer>>());
                }
                sumMap.get(sum).add(jd);
            }
            sumValues[index] = new int[sumMap.keySet().size()];
            removedValues[index] = new HashSet<>();
            int count = 0;
            for (int sumV : sumMap.keySet()) {
                sumValues[index][count] = sumV;
                count++;
            }
            Arrays.sort(sumValues[index]);

            originalJDSumMapping.add(sumMap);
        }
    }

    int[][] sumValues;

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
    private int calculateAvaliableFrequency(ArrayList<ArrayList<Integer>> calculatedRV, int frequency,
            ArrayList<Integer>[] currentlyUsingOriginalJDs) {
        for (int i = 0; i < calculatedRV.size(); i++) {
            String srcTable = referencedTables.get(i);
            int index = jointDegreeAvaIndexes.get(i);
            frequency = Math.min(frequency, jointDegreeAvaStats.get(srcTable).get(calculatedRV.get(i)).ckAvaCount[index]);
            if (jointDegreeAvaStats.get(srcTable).get(calculatedRV.get(i)).ckAvaCount[index] == 0) {
                if (norm1JointDegreeMappingList.get(i).containsKey(currentlyUsingOriginalJDs[i])) {
                    norm1JointDegreeMappingList.get(i).get(currentlyUsingOriginalJDs[i]).remove(calculatedRV.get(i));
                }
            }
        }
        return frequency;
    }

    /**
     * Calculate the incremental frequency for uniqueness constraint
     *
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
     *
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
    private void cleanNorm1AndDistanceMap(ArrayList<ArrayList<Integer>> calculatedRV, ArrayList<Integer>[] currentlyUsingOriginalJDs) {
        for (int k = 0; k < calculatedRV.size(); k++) {
            String sourceTable = this.referencedTables.get(k);
            int index = jointDegreeAvaIndexes.get(k);
            int v = jointDegreeAvaStats.get(sourceTable).get(calculatedRV.get(k)).ckAvaCount[index];
            if (v == 0) {
                cleanNorm1Map(sourceTable, currentlyUsingOriginalJDs, calculatedRV, k);
                ///cleanDistanceMap(k, calculatedRV.get(k));
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
    private void cleanNorm1Map(String sourceTable, ArrayList<Integer>[] currentlyUsingOriginalJDs, ArrayList<ArrayList<Integer>> calculatedRV, int k) {
        if (this.norm1JointDegreeMappingList.get(k).containsKey(currentlyUsingOriginalJDs[k])) {
            if (this.norm1JointDegreeMappingList.get(k).get(currentlyUsingOriginalJDs[k]).contains(calculatedRV.get(k))) {
                this.norm1JointDegreeMappingList.get(k).get(currentlyUsingOriginalJDs[k]).remove(calculatedRV.get(k));
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
        for (int num : jointDegree) {
            sum += num;
        }
        return sum;
    }

    HashSet<Integer>[] removedValues;

    private ArrayList<Integer> calculateClosestJointDegree(ArrayList<Integer> originalJD, int index,
            ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> originalJDSumMapping, ArrayList<ComKey> normKeys,
            ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList) {

        cleanRemovedSums(index, norm1JointDegreeMappingList, originalJDSumMapping);

        ArrayList<Integer> closestJD = calculateClosestJDThroughLooping(index, originalJD, originalJDSumMapping, norm1JointDegreeMappingList);

        if (closestJD.isEmpty()) {
            cleanNorm1MapEmptyEntries(index, norm1JointDegreeMappingList);
            cleanOriginalJDSumMapping(originalJDSumMapping, index);
        }
        return closestJD;
    }

    private ArrayList<ArrayList<Integer>> extractFromMapping(int index, ArrayList<Integer> originalJD) {
        ArrayList<ArrayList<Integer>> cloesestJDs = new ArrayList<>();
        if (norm1JointDegreeMappingList.get(index).containsKey(originalJD)) {
            for (ArrayList<Integer> jointDegree : norm1JointDegreeMappingList.get(index).get(originalJD)) {
                cloesestJDs.add(jointDegree);
            }
        }
        return cloesestJDs;
    }

    private void cleanNorm1MapEmptyEntries(int index, ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMapping) {
        ArrayList<ArrayList<Integer>> removedLists = new ArrayList<>();
        for (ArrayList<Integer> candidateJD : norm1JointDegreeMapping.get(index).keySet()) {
            if (norm1JointDegreeMapping.get(index).get(candidateJD).size() == 0) {
                removedLists.add(candidateJD);
            }
        }

        for (ArrayList<Integer> candidateJD : removedLists) {
            norm1JointDegreeMapping.get(index).remove(candidateJD);
        }
    }

    private void cleanOriginalJDSumMapping(ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> originalJDSumMapping, int index) {
        ArrayList<Integer> removed = new ArrayList<>();
        for (int i : originalJDSumMapping.get(index).keySet()) {
            if (originalJDSumMapping.get(index).get(i).size() == 0) {
                removed.add(i);
            }
        }
        for (int i : removed) {
            originalJDSumMapping.get(index).remove(i);
        }

    }

    private void updateRVNormMap(ArrayList<ArrayList<Integer>> originalRV, ArrayList<ArrayList<Integer>> calculatedRV) {
        if (!norm1RVMapping.containsKey(originalRV)) {
            norm1RVMapping.put(originalRV, new ArrayList<ArrayList<ArrayList<Integer>>>());
        }
        if (!norm1RVMapping.get(originalRV).contains(calculatedRV)) {
            norm1RVMapping.get(originalRV).add(calculatedRV);
        }
    }

    ArrayList<ArrayList<ArrayList<Integer>>> rvList = new ArrayList<>();

    private void generateRVList() {
        // rvList = new ArrayList<>(originalRVDistribution.keySet());
        for (ArrayList<ArrayList<Integer>> originalRV : originalCoDa.rvDistribution.get(curTable).keySet()) {
            rvList.add(originalRV);
        }
    }

    private void cleanRemovedSums(int index, ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList, ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> originalJDSumMapping) {
        if (removedValues[index].size() > 0.1 * sumValues[index].length) {
            int[] tempValues = new int[sumValues[index].length - removedValues[index].size()];
            int count = 0;
            for (int i : sumValues[index]) {
                if (!removedValues[index].contains(i)) {
                    tempValues[count] = i;
                    count++;
                }
            }
            sumValues[index] = tempValues;
            Arrays.sort(sumValues[index]);
            removedValues[index].clear();
            cleanNorm1MapEmptyEntries(index, norm1JointDegreeMappingList);
            cleanOriginalJDSumMapping(originalJDSumMapping, index);
        }
    }

    private int calculateMinIndex(int index, int newSum, ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> originalJDSumMapping, ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList, ArrayList<Integer> originalJD) {
        int minDistance = Integer.MAX_VALUE;
        int minIndex = -1;
        for (int j = 0; j < originalJDSumMapping.get(index).get(newSum).size(); j++) {
            ArrayList<Integer> candidateJD = originalJDSumMapping.get(index).get(newSum).get(j);
            if (!norm1JointDegreeMappingList.get(index).containsKey(candidateJD)
                    || norm1JointDegreeMappingList.get(index).get(candidateJD).size() == 0) {
                continue;
            }
            int distance = calculateDistance(originalJD, candidateJD);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = j;
            }
        }
        return minIndex;
    }

    private ArrayList<Integer> calculateClosestJDThroughLooping(int index, ArrayList<Integer> originalJD, ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> originalJDSumMapping, ArrayList<HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMappingList) {
        int originalSum = calculateDegreeSum(originalJD);
        int matchingIndex = Math.abs(Arrays.binarySearch(sumValues[index], originalSum));

        for (int i = 0; i < sumValues[index].length; i++) {
            for (int k = -1; k <= 1; k = k + 2) {
                int newMatchingIndex = matchingIndex + k * i;
                if (newMatchingIndex < 0 || newMatchingIndex >= sumValues[index].length) {
                    continue;
                }

                int newSum = sumValues[index][newMatchingIndex];
                if (originalJDSumMapping.get(index).containsKey(newSum) && originalJDSumMapping.get(index).get(newSum).size() == 0) {
                    originalJDSumMapping.get(index).remove(newSum);
                    removedValues[index].add(newSum);
                } else if (originalJDSumMapping.get(index).containsKey(newSum)) {
                    int minIndex = calculateMinIndex(index, newSum, originalJDSumMapping, norm1JointDegreeMappingList, originalJD);
                    if (minIndex != -1) {
                        return originalJDSumMapping.get(index).get(newSum).get(minIndex);
                    } else {
                        originalJDSumMapping.get(index).remove(newSum);
                    }
                }
            }
        }
        return new ArrayList<Integer>();
    }

}
