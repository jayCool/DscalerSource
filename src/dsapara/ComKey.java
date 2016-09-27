/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dsapara;

/**
 *
 * @author workshop
 */
public class ComKey {
    String sourceTable;
    String referencingTable;
    int referenceposition;

    @Override
    public String toString() {
      return this.sourceTable+"\t"+this.referencingTable;
    }
    
}
