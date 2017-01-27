/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.ComKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author jiangwei
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
    ArrayList<HashMap<Integer, Integer>> scaledIdFreqs;
    HashMap<ArrayList<Integer>, Integer> originalJDDis;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree;
    HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> mapped = new HashMap<>();

    //The approach of the correlation is based on the original degree distribution.
    // 1. sort the degree in descending order. And settle the originalJDDis from largest to the smallest
    // 2. Find the closest if the originalJDDis is not found
    public JDCorrelation() {
    }

    JDCorrelation(HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            ArrayList<ComKey> jointDegreeTables, ArrayList<HashMap<Integer, Integer>> scaledIdFreqs,
            HashMap<ArrayList<Integer>, Integer> originalJDDis) {
        this.jointDegreeTables = jointDegreeTables;
        this.scaledIdFreqs = scaledIdFreqs;
        this.scaledJDDis = scaledJDDis;
        this.originalJDDis = originalJDDis;
    }

    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> corrMap(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution, HashMap<ArrayList<String>, HashMap<Integer, Integer>> downsizedCounts) {
        // HashMap<ArrayList<ArrayList<Integer>>, Integer> correlatedOriginal = loadCorrAll(corrOriginal);
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> result = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : mergedDistribution.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> arrs = new ArrayList<>();
            for (int i = 1; i < entry.getKey().size(); i++) {
                ArrayList<String> arr = new ArrayList<>();
                arr.add(entry.getKey().get(0));
                arr.add(entry.getKey().get(i));
                arrs.add(downsizedCounts.get(arr));
            }
            //      System.out.println("start");
            //      System.out.println(degree.getKey());
            HashMap<ArrayList<Integer>, Integer> correlated = jointDegreeCorr(entry.getValue(), arrs);
            result.put(entry.getKey(), correlated);
            //   System.out.println("end");
        }
        return result;

    }

    private HashMap<ArrayList<Integer>, Integer> jointDegreeCorr(HashMap<ArrayList<Integer>, Integer> originalJDDis,
            ArrayList<HashMap<Integer, Integer>> scaledIdFreqs) {
        HashMap<ArrayList<Integer>, Integer> scaledOneJDDis = correlation(originalJDDis, scaledIdFreqs);
        iterations++;

        while (!scaledIdFreqs.get(0).keySet().isEmpty()) {
            HashMap<ArrayList<Integer>, Integer> extraScaledOneJDDis = correlation(originalJDDis, scaledIdFreqs);
            mergeExtraJDDis(extraScaledOneJDDis, scaledOneJDDis);
        }

        this.mappedBestJointDegree.put(jointDegreeTables, mapped);
        return scaledOneJDDis;
    }

    private HashMap<ArrayList<Integer>, Integer> correlation(HashMap<ArrayList<Integer>, Integer> originalJDDis,
            ArrayList<HashMap<Integer, Integer>> scaledIdFreqs) {

        clearEmptyScaleIDFreq(scaledIdFreqs);

        HashMap<ArrayList<Integer>, Integer> scaledOneJDDis = new HashMap<>();
        int frequency = 0;
        List<Entry<ArrayList<Integer>, Integer>> sortedJDDis = new Sort().sortOnKeySum1(originalJDDis);

        for (int i = 0; i < sortedJDDis.size() && !scaledIdFreqs.get(0).keySet().isEmpty() && sortedJDDis.size() > 0; i++) {
            Entry<ArrayList<Integer>, Integer> jdFreqEntry = sortedJDDis.get(i);
            ArrayList<Integer> calJDs = new ArrayList<>();

            // find the closest matching joint degree
            boolean notFound = calCandidateJD(jdFreqEntry, scaledIdFreqs, calJDs);
            if (notFound) {
                continue;
            }
            if (emptyScaledIDFreqs) {
                break;
            }

            frequency = calFreq(jdFreqEntry);
            for (int j = 0; j < calJDs.size() && frequency > 0; j++) {
                frequency = Math.min(frequency, scaledIdFreqs.get(j).get(calJDs.get(j)));
            }
            if (frequency <= 0) {
                continue;
            }

            storeJDMapping(jdFreqEntry, calJDs);
            boolean cleaningNeeded = updateDistributions(calJDs, scaledIdFreqs, scaledOneJDDis, frequency);
            if (cleaningNeeded) {
                cleanZeroBudgetDis(scaledIdFreqs, calJDs);
            }

        }
        return scaledOneJDDis;
    }

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
        scaledJDDis.put(jointDegreeTables, jointDegreeCorr(originalJDDis, scaledIdFreqs));
        threadTerminated = true;
    }

    private boolean checkNotFound(Entry<ArrayList<Integer>, Integer> jdFreqEntry,
            ArrayList<HashMap<Integer, Integer>> scaledIdFreqs) {

        for (int j = 0; j < jdFreqEntry.getKey().size(); j++) {
            if (scaledIdFreqs.get(j).keySet().size() == 0) {
                emptyScaledIDFreqs = true;
                return true;
            }
            if (!scaledIdFreqs.get(j).containsKey(jdFreqEntry.getKey().get(j))) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<Integer> calJointDegrees(Entry<ArrayList<Integer>, Integer> jdFreqEntry,
            ArrayList<HashMap<Integer, Integer>> scaledIdFreqs) {
        ArrayList<Integer> calJDs = new ArrayList<>();

        for (int j = 0; j < jdFreqEntry.getKey().size(); j++) {
            if (scaledIdFreqs.get(j).keySet().size() == 0) {
                emptyScaledIDFreqs = true;
                break;
            }
            int closeDeg = findClosestDegree(jdFreqEntry.getKey().get(j), scaledIdFreqs.get(j).keySet());
            calJDs.add(closeDeg);
        }
        return calJDs;
    }

    private int calFreq(Entry<ArrayList<Integer>, Integer> jdFreqEntry) {
        int frequency = (int) Math.floor(jdFreqEntry.getValue() * sRatio);
        double diff = jdFreqEntry.getValue() * sRatio - frequency;
        double kl = Math.random();
        if (kl < diff) {
            frequency++;
        }
        return frequency;
    }

    private void storeJDMapping(Entry<ArrayList<Integer>, Integer> jdFreqEntry, ArrayList<Integer> calJDs) {
        if (!mapped.containsKey(jdFreqEntry.getKey())) {
            mapped.put(jdFreqEntry.getKey(), new ArrayList<ArrayList<Integer>>());
        }
        if (!mapped.get(jdFreqEntry.getKey()).contains(calJDs)) {
            mapped.get(jdFreqEntry.getKey()).add(calJDs);
        }
    }

    void initialize(double s, HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree) {
        this.sRatio = s;
        this.mappedBestJointDegree = mappedBestJointDegree;
    }

    private void clearEmptyScaleIDFreq(ArrayList<HashMap<Integer, Integer>> scaledIdFreqs) {
        for (int i = 0; i < scaledIdFreqs.size(); i++) {
            ArrayList<Integer> keySetCopy = new ArrayList<Integer>(scaledIdFreqs.get(i).keySet());
            for (int degree : keySetCopy) {
                if (scaledIdFreqs.get(i).get(degree) == 0) {
                    scaledIdFreqs.get(i).remove(degree);
                }
            }
        }
    }

    private boolean calCandidateJD(Entry<ArrayList<Integer>, Integer> jdFreqEntry, ArrayList<HashMap<Integer, Integer>> scaledIdFreqs, ArrayList<Integer> calJDs) {
        boolean notfound = false;
        if (iterations == 0) {
            notfound = checkNotFound(jdFreqEntry, scaledIdFreqs);
            calJDs = jdFreqEntry.getKey();
        } else {
            calJDs = calJointDegrees(jdFreqEntry, scaledIdFreqs);
        }
        return notfound;
    }

    private boolean updateDistributions(ArrayList<Integer> calJDs, ArrayList<HashMap<Integer, Integer>> scaledIdFreqs, HashMap<ArrayList<Integer>, Integer> scaledOneJDDis, int frequency) {
        int newBudget = 0;
        for (int j = 0; j < calJDs.size(); j++) {
            int totalBudget = scaledIdFreqs.get(j).get(calJDs.get(j));
            newBudget = totalBudget - frequency;
            scaledIdFreqs.get(j).put(calJDs.get(j), newBudget);
            if (newBudget == 0) {
                return true;
            }
        }

        if (!scaledOneJDDis.containsKey(calJDs)) {
            scaledOneJDDis.put(calJDs, 0);
        }
        scaledOneJDDis.put(calJDs, frequency + scaledOneJDDis.get(calJDs));
        return false;
    }

    private void cleanZeroBudgetDis(ArrayList<HashMap<Integer, Integer>> scaledIdFreqs, ArrayList<Integer> calJDs) {
        for (int j = 0; j < scaledIdFreqs.size(); j++) {
            if (scaledIdFreqs.get(j).containsKey(calJDs.get(j)) && scaledIdFreqs.get(j).get(calJDs.get(j)) == 0) {
                scaledIdFreqs.get(j).remove(calJDs.get(j));
            }
        }
    }

    private void mergeExtraJDDis(HashMap<ArrayList<Integer>, Integer> extraScaledOneJDDis, HashMap<ArrayList<Integer>, Integer> scaledOneJDDis) {
        for (Entry<ArrayList<Integer>, Integer> entry : extraScaledOneJDDis.entrySet()) {
            if (!scaledOneJDDis.containsKey(entry.getKey())) {
                scaledOneJDDis.put(entry.getKey(), 0);
            }
            scaledOneJDDis.put(entry.getKey(), scaledOneJDDis.get(entry.getKey()) + entry.getValue());
        }
    }

}
