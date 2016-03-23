package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.KryoSerializable;

/**
 * Represents a Cumulative Moving Average value storing data for 10 iterations and provides methods
 * for easy update and calculation of the CMA
 * @author atanaspam
 * @version 0.1
 * @created 17/02/2016
 */
public class Last10CMA {

    int cumulativeMovingAverage;
    int[] oldValues;
    int index;

    public Last10CMA(int value, int numberOfDatumPoints) {
        index = 0;
        this.oldValues = new int[10];
        oldValues[index++] = value;
        this.cumulativeMovingAverage = value;
    }
    public Last10CMA(){
        index =0;
        this.cumulativeMovingAverage = 0;
        this.oldValues = new int[10];
    }

    /**
     * Adds a new value to the cumulative Moving Average until now
     * @param value the new value to be added
     * @return true if the new value is larger than the CMA before the update, false otherwise
     */
    public void addValue(int value) {
        cumulativeMovingAverage = (cumulativeMovingAverage - oldValues[index]) + value;
        oldValues[index] = value;
        index++;
        if (index == oldValues.length){
            index = 0;
        }
    }

    public void setCumulativeMovingAverage(int cumulativeMovingAverage) {
        this.cumulativeMovingAverage = cumulativeMovingAverage;
    }
    public int getCumulativeMovingAverage() {
        return cumulativeMovingAverage/10;
    }

    /**
     * Resets the state of the CMA
     */
    public void clear(){
        cumulativeMovingAverage = 0;
         for (int i=0; i< oldValues.length; i++){
             oldValues[i] = 0;
         }
    }

    @Override
    public String toString() {
        return "CMA=" + cumulativeMovingAverage/10;
    }
}
