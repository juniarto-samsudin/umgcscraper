/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

/**
 *
 * @author samsudinj
 */
public class SpeedBandThreadResult {
    private boolean isSorted;
    private int numberOfLink;
    private int contSignal;
    private String filename;
    private int skip;
    public SpeedBandThreadResult (boolean isSorted, int numberOfLink, int contSignal, String filename, int skip){
        this.isSorted = isSorted;
        this.numberOfLink = numberOfLink;
        this.contSignal = contSignal;
        this.filename = filename;
        this.skip = skip;
    }
    
    public boolean getIsSorted(){
        return isSorted;
    }
    
    public int getNumberOfLink(){
        return numberOfLink;
    }
    
    public int getContSignal(){
        return contSignal;
    }
    
    public String getFileName(){
        return filename;
    }
    
    public int getSkip(){
        return skip;
    }
    
}
