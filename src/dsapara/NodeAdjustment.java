/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class NodeAdjustment {

    /**
     * This method returns the sum of the elements in x
     *
     * @param x
     * @return
     */
    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }

    /**
     * This method does the following: (1) smoothing the degrees, example
     * [1,3,4,5,7] ==> [1,2,3,4,5,6,7] (2) extending the largest degree
     *
     * @param degreeList
     * @param frequencies
     */
    private void smoothAndExtendDegrees(ArrayList<Integer> degreeList, ArrayList<Integer> frequencies) {
        int minDegree = degreeList.get(0);
        int maxDegree = degreeList.get(degreeList.size() - 1);
        HashMap<Integer, Integer> degreeFreq = new HashMap<>();
        for (int index = 0; index < degreeList.size(); index++) {
            degreeFreq.put(degreeList.get(index), frequencies.get(index));
        }
        degreeList.clear();
        frequencies.clear();
        for (int i = minDegree; i <= maxDegree; i++) {
            degreeList.add(i);
            if (!degreeFreq.containsKey(i)) {
                frequencies.add(0);
            } else {
                frequencies.add(degreeFreq.get(i));
            }
        }

        for (int i = 0; i < Math.min(Constant.CLEANING_THRESHOLD, maxDegree / Constant.CLEANING_THRESHOLD); i++) {
            degreeList.add(degreeList.size());
            frequencies.add(0);
        }
    }

    /**
     * This method adjusts the degree distribution to make sure it satisfies the
     * scaledNodeSize.
     *
     * @param degreeDis
     * @param scaledNodeSize
     */
    public HashMap<Integer, Integer> adjustment(HashMap<Integer, Integer> degreeDis, int scaledNodeSize) {
        System.err.println("extract degree list");
        ArrayList<Integer> degreeList = extractDegreeList(degreeDis);

        System.err.println("calculate frequencies");

        ArrayList<Integer> frequencies = coordinateTheFrequencies(degreeDis, degreeList);
        /**
         * test
         */
      //  test(frequencies, degreeList);

        System.err.println("smoothing: ");
        smoothAndExtendDegrees(degreeList, frequencies);

      //  test(frequencies, degreeList);

        System.err.println("even Distribution:");

        evenDistributionOfDiffs(scaledNodeSize, frequencies);

     //   test(frequencies, degreeList);

        System.out.println("random swap");
        randomAdjustmentForDiffs(frequencies, scaledNodeSize);

    //    test(frequencies, degreeList);

        degreeDis.clear();

        for (int i = 0; i < degreeList.size(); i++) {
            degreeDis.put(degreeList.get(i), frequencies.get(i));
        }

        frequencies.clear();
        degreeList.clear();
        return degreeDis;
    }

    /**
     * This method extract the sorted degrees, [1,2,3,4,6,...]
     *
     * @param degreeDis
     * @return sorted degree list
     */
    private ArrayList<Integer> extractDegreeList(HashMap<Integer, Integer> degreeDis) {
        ArrayList<Integer> degreeList = new ArrayList<>();
        for (int degree : degreeDis.keySet()) {
            degreeList.add(degree);
        }
        Collections.sort(degreeList);
        return degreeList;
    }

    /**
     * Given the degree list, and degree frequency HashMap, it returns the
     * frequency list.
     *
     * @param degreeDis
     * @param degreeList
     * @return ordered frequencies
     */
    private ArrayList<Integer> coordinateTheFrequencies(HashMap<Integer, Integer> degreeDis, ArrayList<Integer> degreeList) {
        ArrayList<Integer> frequencies = new ArrayList<>();
        for (int degree : degreeList) {
            if (!degreeDis.containsKey(degree)) {
                frequencies.add(0);
            } else {
                frequencies.add(degreeDis.get(degree));
            }
        }
        return frequencies;
    }

    /**
     * Distribute the diffs proportionally to the frequencies
     *
     * @param scaledNodeSize
     * @param frequencies
     */
    private void evenDistributionOfDiffs(int scaledNodeSize, ArrayList<Integer> frequencies) {
        int vertexSum = sumVector(frequencies);

        int diffs = scaledNodeSize - vertexSum;
        for (int i = 0; i < frequencies.size(); i++) {
            double ratio = (int) 1.0 * frequencies.get(i) / vertexSum;
            frequencies.set(i, Math.max(0, (int) (ratio * diffs) + frequencies.get(i)));
        }
    }

    /**
     * Randomly pick up the degrees, and modifies the frequencies by 1.
     *
     * @param frequencies
     * @param scaledNodeSize
     */
    private void randomAdjustmentForDiffs(ArrayList<Integer> frequencies, int scaledNodeSize) {
        int vertexSum = sumVector(frequencies);
        int diffs = scaledNodeSize - vertexSum;
        int adjustingIndex = 0;
        System.err.println(" diff: " + diffs);
        ArrayList<Integer> positiveNodes = calPositiveNodes(frequencies);
        if (diffs > 0) {
            while (diffs != 0) {
                frequencies.set(adjustingIndex,  frequencies.get(adjustingIndex) + Math.abs(diffs) / diffs);
                adjustingIndex = (adjustingIndex + 1) % (frequencies.size());
                diffs --;
            }
        }else{
             while (diffs != 0) {
                int posIndex = positiveNodes.get(adjustingIndex);
                frequencies.set(posIndex,  frequencies.get(posIndex) + Math.abs(diffs) / diffs);
                if (frequencies.get(adjustingIndex)==0){
                    positiveNodes.remove(adjustingIndex);
                }
                adjustingIndex = (adjustingIndex + 1) % (positiveNodes.size());
                diffs ++;
            }
        }

    }

    private void test(ArrayList<Integer> frequencies, ArrayList<Integer> degreeList) {
        HashMap<Integer, Integer> positiveHashMap = new HashMap<>();
        for (int i = 0; i < degreeList.size(); i++) {
            if (frequencies.get(i) > 0) {
                positiveHashMap.put(degreeList.get(i), frequencies.get(i));
            }
        }
        System.err.println("positiveMap: " + positiveHashMap);
    }

    private ArrayList<Integer> calPositiveNodes(ArrayList<Integer> frequencies) {
        ArrayList<Integer> result = new ArrayList<>();
        for (int index = 0; index < frequencies.size(); index++) {
            if (frequencies.get(index) > 0) {
                result.add(index);
            }
        }
        return result;
    }

}
