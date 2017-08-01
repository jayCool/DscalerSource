/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import db.structs.ComKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author Zhang Jiangwei
 *
 *
 */
public class JDCorrelation extends PrintFunction implements Runnable {

    public double sRatio = 0.2;
    boolean threadTerminated = false;
    int iterations = 0;
    boolean emptyScaledIDFreqs = false;

    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis;
    ArrayList<ComKey> jointDegreeTables;
    ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions;
    HashMap<ArrayList<Integer>, ArrayList<Integer>> originalReverseJointDegrees;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> norm1JointDegreeMapping;
    HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> jointDegreeMapping = new HashMap<>();

     JDCorrelation(HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            ArrayList<ComKey> jointDegreeTables, ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions,
            HashMap<ArrayList<Integer>, ArrayList<Integer>> originalReverseJointDegrees) {
        this.jointDegreeTables = jointDegreeTables;
        this.scaledDegreeDistributions = scaledDegreeDistributions;
        this.scaledJDDis = scaledJDDis;
        this.originalReverseJointDegrees = originalReverseJointDegrees;
    }

     
      //The approach of the correlation is based on the original degree distribution.
    // 1. sort the degree in descending order. And settle the originalReverseJointDegrees from largest to the smallest
    // 2. Find the closest if the originalReverseJointDegrees is not found
  
    private HashMap<ArrayList<Integer>, Integer> jointDegreeCorrelation(HashMap<ArrayList<Integer>, ArrayList<Integer>> originalReverseJointDegrees,
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions) {
        List<Entry<ArrayList<Integer>, ArrayList<Integer>>> sortedOriginalJDDis = new Sort().sortOnKeySumJD(originalReverseJointDegrees);

        HashMap<ArrayList<Integer>, Integer> scaledJDDis = jdCorrelationOneLoop(sortedOriginalJDDis, scaledDegreeDistributions);
        iterations++;

        while (!scaledDegreeDistributions.get(0).keySet().isEmpty()) {
            HashMap<ArrayList<Integer>, Integer> extraScaledOneJDDis = jdCorrelationOneLoop(sortedOriginalJDDis, scaledDegreeDistributions);
            mergeExtraJDDis(extraScaledOneJDDis, scaledJDDis);
        }

        this.norm1JointDegreeMapping.put(jointDegreeTables, jointDegreeMapping);
        return scaledJDDis;
    }

    /**
     * This method returns the incremental synthesized joint-degree distribution
     *
     * @param sortedOriginalJDDis
     * @param scaledDegreeDistributions
     * @return
     */
    private HashMap<ArrayList<Integer>, Integer> jdCorrelationOneLoop(
            List<Entry<ArrayList<Integer>, ArrayList<Integer>>> sortedOriginalJDDis,
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions) {

        clearEmptyscaledDegreeDistributions(scaledDegreeDistributions);

        HashMap<ArrayList<Integer>, Integer> scaledOneLoopJDDis = new HashMap<>();

        for (int i = 0; i < sortedOriginalJDDis.size() && !scaledDegreeDistributions.get(0).keySet().isEmpty() && sortedOriginalJDDis.size() > 0; i++) {
            Entry<ArrayList<Integer>, ArrayList<Integer>> originalJDEntry = sortedOriginalJDDis.get(i);

            // find the closest joint degree
            ArrayList<Integer> calculatedJD = new ArrayList<>();
            boolean firstIterAndNoPerfectMatch = calCandidateJD(originalJDEntry, scaledDegreeDistributions, calculatedJD);
            if (firstIterAndNoPerfectMatch) {
                continue;
            }
            if (emptyScaledIDFreqs) {
                break;
            }

            int frequency = calculateFrequency(originalJDEntry);

            //Find the minimal avaliable frequency from scaledDegreeDistributions
            for (int j = 0; j < calculatedJD.size() && frequency > 0; j++) {
                frequency = Math.min(frequency, scaledDegreeDistributions.get(j).get(calculatedJD.get(j)));
            }

            if (frequency <= 0) {
                continue;
            }

            storeJDMapping(originalJDEntry, calculatedJD);

            updateStatistics(calculatedJD, scaledDegreeDistributions, scaledOneLoopJDDis, frequency);
        }
        
        clearEmptyscaledDegreeDistributions(scaledDegreeDistributions);
        return scaledOneLoopJDDis;
    }

    
    /**
     * Find the closest degree from degreeSet
     * @param degree
     * @param degreeSet
     * @return closest degree
     */
    private int findClosestDegree(Integer degree, Set<Integer> degreeSet) {
        if (degreeSet.size() == 0) {
            return 0;
        }
        if (degreeSet.contains(degree)) {
            return degree;
        }
        int min = Integer.MAX_VALUE;
        int result = 0;

        for (int candidateDegree : degreeSet) {
            if (Math.abs(candidateDegree - degree) < min) {
                min = Math.abs(candidateDegree - degree);
                result = candidateDegree;
            }
        }

        return result;
    }

    @Override
    public void run() {
        scaledJDDis.put(jointDegreeTables, jointDegreeCorrelation(originalReverseJointDegrees, scaledDegreeDistributions));
        threadTerminated = true;
    }

    /**
     * This method checks if the scaledDegreeDistributions can form the
     * originalJDEntry
     *
     * @param originalJDEntry
     * @param scaledDegreeDistributions
     * @return True:No perfect matching, False:Perfect Matching
     */
    private boolean checkPerfectMatch(Entry<ArrayList<Integer>, ArrayList<Integer>> originalJDEntry,
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions) {

        for (int j = 0; j < originalJDEntry.getKey().size(); j++) {
            if (scaledDegreeDistributions.get(j).keySet().size() == 0) {
                emptyScaledIDFreqs = true;
                return true;
            }
            if (!scaledDegreeDistributions.get(j).containsKey(originalJDEntry.getKey().get(j))) {
                return true;
            }
        }
        return false;
    }

    
    /**
     * 
     * @param originalJDEntry
     * @param scaledDegreeDistributions
     * @param calculatedJD
     * @return closestJointDegree
     */
    private ArrayList<Integer> calJointDegrees(Entry<ArrayList<Integer>, ArrayList<Integer>> originalJDEntry,
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions, ArrayList<Integer> calculatedJD) {

        for (int j = 0; j < originalJDEntry.getKey().size(); j++) {
            if (scaledDegreeDistributions.get(j).keySet().size() == 0) {
                emptyScaledIDFreqs = true;
                break;
            }
            int closestDegree = findClosestDegree(originalJDEntry.getKey().get(j), scaledDegreeDistributions.get(j).keySet());
            calculatedJD.add(closestDegree);
        }
        return calculatedJD;
    }

    /**
     * Calculate the expected frequency
     *
     * @param originalJDEntry
     * @return expected frequency
     */
    private int calculateFrequency(Entry<ArrayList<Integer>, ArrayList<Integer>> originalJDEntry) {
        int frequency = (int) Math.floor(originalJDEntry.getValue().size() * sRatio);
        double diff = originalJDEntry.getValue().size() * sRatio - frequency;
        if (Math.random() < diff) {
            frequency++;
        }
        return frequency;
    }

    /**
     * Store the closest joint-degree mapping This will be used in RVC and KVC
     *
     * @param originalJDEntry
     * @param calculatedJD
     */
    private void storeJDMapping(Entry<ArrayList<Integer>, ArrayList<Integer>> originalJDEntry, ArrayList<Integer> calculatedJD) {
        if (!jointDegreeMapping.containsKey(originalJDEntry.getKey())) {
            jointDegreeMapping.put(originalJDEntry.getKey(), new ArrayList<ArrayList<Integer>>());
        }
        if (!jointDegreeMapping.get(originalJDEntry.getKey()).contains(calculatedJD)) {
            jointDegreeMapping.get(originalJDEntry.getKey()).add(calculatedJD);
        }
    }

    /**
     * Initialize parameters
     * @param sRatio
     * @param mappedBestJointDegree 
     */
    void initialize(double sRatio, HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree) {
        this.sRatio = sRatio;
        this.norm1JointDegreeMapping = mappedBestJointDegree;
    }

    /**
     * Clear the distribution with 0 budget or null
     * @param scaledDegreeDistributions 
     */
    private void clearEmptyscaledDegreeDistributions(ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions) {
        for (int i = 0; i < scaledDegreeDistributions.size(); i++) {
            if (scaledDegreeDistributions.get(i) == null) {
                continue;
            }
            ArrayList<Integer> keySetCopy = new ArrayList<Integer>(scaledDegreeDistributions.get(i).keySet());
            for (int degree : keySetCopy) {
                if (scaledDegreeDistributions.get(i).get(degree) == 0) {
                    scaledDegreeDistributions.get(i).remove(degree);
                }
            }
        }
    }

    
    /**
     * This method finds the calculatedJointdegree
     * And returns if firstIterAndNoPerfectMatch
     * @param originalJDEntry
     * @param scaledDegreeDistributions
     * @param calculatedJD
     * @return 
     */
    private boolean calCandidateJD(Entry<ArrayList<Integer>, ArrayList<Integer>> originalJDEntry,
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions, ArrayList<Integer> calculatedJD) {
        boolean firstIterAndNoPerfectMatch = false;
        if (iterations == 0) {
            firstIterAndNoPerfectMatch = checkPerfectMatch(originalJDEntry, scaledDegreeDistributions);
            for (int i : originalJDEntry.getKey()) {
                calculatedJD.add(i);
            }
        } else {
            calJointDegrees(originalJDEntry, scaledDegreeDistributions, calculatedJD);
        }
        return firstIterAndNoPerfectMatch;
    }

    /**
     * This method updates the related statistics
     *
     * @param calculatedJD
     * @param scaledDegreeDistributions
     * @param scaledOneLoopJDDis
     * @param frequency
     * @return
     */
    private void updateStatistics(ArrayList<Integer> calculatedJD,
            ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions,
            HashMap<ArrayList<Integer>, Integer> scaledOneLoopJDDis, int frequency) {
        int newBudget = 0;
        boolean cleanning = false;
        for (int j = 0; j < calculatedJD.size(); j++) {
            int oldBudget = scaledDegreeDistributions.get(j).get(calculatedJD.get(j));
            newBudget = oldBudget - frequency;
            scaledDegreeDistributions.get(j).put(calculatedJD.get(j), newBudget);
            if (newBudget == 0) {
                cleanning = true;
            }
        }

        if (!scaledOneLoopJDDis.containsKey(calculatedJD)) {
            scaledOneLoopJDDis.put(calculatedJD, 0);
        }
        scaledOneLoopJDDis.put(calculatedJD, frequency + scaledOneLoopJDDis.get(calculatedJD));

        if (cleanning) {
            cleanZeroBudgetDegreeDistribution(scaledDegreeDistributions, calculatedJD);
        }
        return;
    }

    /**
     * Cleans the degreedistribution with 0 budget
     *
     * @param scaledDegreeDistributions
     * @param calculatedJD
     */
    private void cleanZeroBudgetDegreeDistribution(ArrayList<HashMap<Integer, Integer>> scaledDegreeDistributions, ArrayList<Integer> calculatedJD) {
        for (int j = 0; j < scaledDegreeDistributions.size(); j++) {
            if (scaledDegreeDistributions.get(j).containsKey(calculatedJD.get(j)) && scaledDegreeDistributions.get(j).get(calculatedJD.get(j)) == 0) {
                scaledDegreeDistributions.get(j).remove(calculatedJD.get(j));
            }
        }
    }
    
    /**
     * Merge extraScaledOneJDDis to scaledJDDist
     * @param extraScaledOneJDDis
     * @param scaledJDDis 
     */
    private void mergeExtraJDDis(HashMap<ArrayList<Integer>, Integer> extraScaledOneJDDis,
            HashMap<ArrayList<Integer>, Integer> scaledJDDis) {
        for (Entry<ArrayList<Integer>, Integer> entry : extraScaledOneJDDis.entrySet()) {
            if (!scaledJDDis.containsKey(entry.getKey())) {
                scaledJDDis.put(entry.getKey(), 0);
            }
            scaledJDDis.put(entry.getKey(), scaledJDDis.get(entry.getKey()) + entry.getValue());
        }
    }

}
