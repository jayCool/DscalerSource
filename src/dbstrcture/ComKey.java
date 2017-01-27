package dbstrcture;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



/**
 *
 * @author workshop
 */
public class ComKey {
    public String sourceTable;
    public String referencingTable;
    public int referenceposition;
    
    @Override
    public String toString() {
      return this.sourceTable+"\t"+this.referencingTable;
    }
    
}
