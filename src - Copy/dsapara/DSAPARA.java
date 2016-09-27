/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dsapara;

import java.io.IOException;

/**
 *
 * @author workshop
 */
public class DSAPARA {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
 //  FSQ fsq = new FSQ(args[0]);
      //   fsq.s = Double.parseDouble(args[1]);
      //   fsq.delim = args[2];
       //  fsq.ignoreFirst = Boolean.parseBoolean(args[3]);
     FSQ fsq = new FSQ();
   fsq.filePath = "berka/";
     fsq.s=15;
        fsq.run();
    }
    
}
