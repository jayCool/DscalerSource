package main;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

class EdgeAdjust extends Sort {

    long starttime = 0;

    EdgeAdjust(long currentTimeMillis) {
        this.starttime = currentTimeMillis;
    }

    /**
     * This method returns the adjustable diffs and the corresponding pairs. For
     * example, [1,2,3,4] then the pairs are {-3:, -2:, -1:, 1:, 2:, 3:}
     *
     * @param degreeList
     * @param frequencies
     * @return AdjustableDiffMap
     */
    private HashMap<Integer, ArrayList<Integer>> calAdjustableDiffAndDegreePairs(ArrayList<Integer> degreeList, ArrayList<Integer> frequencies) {
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<>();
        for (int i = 0; i < degreeList.size(); i++) {
            if (frequencies.get(i) > 0) {
                /*
                for (int j = i + 1; j < degreeList.size(); j++) {
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(i);
                    arr.add(j);
                    result.put(degreeList.get(j) - degreeList.get(i), arr);
                }
                 */
                for (int j = 0; j < degreeList.size(); j++) {
                    int diff = degreeList.get(j) - degreeList.get(i);
                    if (result.containsKey(diff)) {
                        continue;
                    }
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(i);
                    arr.add(j);
                    result.put(diff, arr);

                }
            }
        }
        return result;
    }

    /**
     * This method modifies the scaledDegree to satisfy the scaledEdgeSize and
     * scaledNodeSize.
     *
     * @param scaleDegree
     * @param scaledEdgeSize
     * @param scaledNodeSize
     * @return scaledDegreeDistribution After Edge Adjustment
     * @throws FileNotFoundException
     */
    HashMap<Integer, Integer> smoothDegree(HashMap<Integer, Integer> scaleDegree, int scaledEdgeSize, int scaledNodeSize){
        ArrayList<Integer> degreeList = new ArrayList<>();
        ArrayList<Integer> frequencies = new ArrayList<>();

        closingDegreeGap(scaleDegree, degreeList, frequencies);

        HashMap<Integer, ArrayList<Integer>> adjustableDiffMap = calAdjustableDiffAndDegreePairs(degreeList, frequencies);
        boolean maxflag = false;

        int edgeDiff = -product(degreeList, frequencies) + scaledEdgeSize;
        int ender = frequencies.size() - 1;
        int starter = 0;

        while (!adjustableDiffMap.containsKey(edgeDiff) && edgeDiff != 0) {
            RunningException.checkTooLongRunTime(starttime);

            if (edgeDiff < 0) {
                if (frequencies.get(ender) > 0) {
                    frequencies.set(starter, frequencies.get(starter) + 1);
                    frequencies.set(ender, frequencies.get(ender) - 1);
                    if (frequencies.get(ender) <= 0) {
                        maxflag = true;
                    }
                    starter++;
                    ender--;
                } else {
                    ender--;
                }
            } else if (frequencies.get(starter) > 0) {
                frequencies.set(starter, frequencies.get(starter) - 1);
                frequencies.set(ender, frequencies.get(ender) + 1);
                if (frequencies.get(starter) <= 0) {
                    maxflag = true;
                }
                starter++;
                ender--;
            } else {
                starter++;
            }

            if (starter >= ender) {
                starter = 0;
                ender = frequencies.size() - 1;
            }

            edgeDiff = scaledEdgeSize - product(degreeList, frequencies);

            if (maxflag) {
                adjustableDiffMap = this.calAdjustableDiffAndDegreePairs(degreeList, frequencies);
                maxflag = false;
            }
        }

        if (edgeDiff != 0) {
            ArrayList<Integer> arr = adjustableDiffMap.get(edgeDiff);
            frequencies.set(arr.get(0), frequencies.get(arr.get(0)) - 1);
            frequencies.set(arr.get(1), frequencies.get(arr.get(1)) + 1);

        }
        HashMap<Integer, Integer> res = new HashMap<>();

        for (int i = 0; i < degreeList.size(); i++) {
            res.put(degreeList.get(i), frequencies.get(i));
        }
        return res;
    }

    /**
     * Calculate the vector product
     *
     * @param x
     * @param value
     * @return The product of two input vector
     */
    private int product(ArrayList<Integer> x, ArrayList<Integer> value) {
        int sum = 0;
        for (int i = 0; i < x.size(); i++) {
            if (x.get(i) > 0 && value.get(i) > 0) {
                sum += x.get(i) * value.get(i);
            }
        }
        return sum;
    }

    /**
     * This method makes sure that the degreeList does not have gap in-between.
     *
     * @param scaleDegree
     * @param degreeList
     * @param frequencies
     */
    private void closingDegreeGap(HashMap<Integer, Integer> scaleDegree, ArrayList<Integer> degreeList, ArrayList<Integer> frequencies) {
        int maxDegree = Collections.max(scaleDegree.keySet());
        int minDegree = Collections.min(scaleDegree.keySet());
        for (int i = minDegree; i < maxDegree; i++) {
            if (!scaleDegree.containsKey(i)) {
                scaleDegree.put(i, 0);
            }
            degreeList.add(i);
            frequencies.add(scaleDegree.get(i));
        }
    }

}
