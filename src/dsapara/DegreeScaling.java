package dsapara;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map.Entry;

public class DegreeScaling {
    
    
    /**
     * 
     * @param originalDegreeDis
     * @param scaledEdgeSize
     * @param scaledNodeSize
     * @param s_n
     * @return scaledDistribution
     * @throws FileNotFoundException 
     */
    public HashMap<Integer, Integer> scale(HashMap<Integer, Integer> originalDegreeDis, int scaledEdgeSize, int scaledNodeSize, double s_n) {
        HashMap<Integer, Integer> scaleDegree = saticScale(originalDegreeDis, s_n);
        NodeAdjustment nodeAdjustment = new NodeAdjustment();
        nodeAdjustment.adjustment(scaleDegree, scaledNodeSize);
        
        EdgeAdjust edgeAdjust = new EdgeAdjust(System.currentTimeMillis());
        HashMap<Integer, Integer> smoothDegree = edgeAdjust.smoothDegree(scaleDegree, scaledEdgeSize, scaledNodeSize);

        return smoothDegree;
    }
    
    
   
    private int calExpectation(double val) {
        int base = (int) val;
        if ((val - base) > Math.random()) {
            base++;
        }
        return base;
    }
    
    
    /**
     * This function statically scale the frequency by s_n, 
     * say the original frequency of degree 1 is 100, 
     * then the scaled frequency will be 100*s_n.
     * 
     * @param originalDegreeDis
     * @param s_n (scaling ratio)
     * @return Scaled degree distribution
     */
    private HashMap<Integer, Integer> saticScale(HashMap<Integer, Integer> originalDegreeDis, double s_n) {
        HashMap<Integer, Integer> results = new HashMap<>();
        
        for (Entry<Integer, Integer> entry : originalDegreeDis.entrySet()) {
            int val = calExpectation(s_n * entry.getValue());
            results.put(entry.getKey(), val);
        }
        return results;
    }

}
