package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Represents a Cumulative Moving Average value and provides methods
 * for easy update and calculation of the CMA
 * @author atanaspam
 * @version 0.1
 * @created 09/02/2016
 */
public class CMA implements KryoSerializable{

    int cumulativeMovingAverage;
    int numberOfDatumPoints;

    public CMA(int cumulativeMovingAverage, int numberOfDatumPoints) {
        this.cumulativeMovingAverage = cumulativeMovingAverage;
        this.numberOfDatumPoints = numberOfDatumPoints;
    }
    public CMA(){
        this.cumulativeMovingAverage = 0;
        this.numberOfDatumPoints = 0;
    }

    /**
     * Adds a new value to the Cumulative Moving Average until now
     * @param value the new value to be added
     * @return true if the new value is larger than the CMA before the update, false otherwise
     */
    public void addValue(int value) {
        this.cumulativeMovingAverage = (value + (numberOfDatumPoints * cumulativeMovingAverage)) / (numberOfDatumPoints + 1);
        numberOfDatumPoints++;
    }

    public void setCumulativeMovingAverage(int cumulativeMovingAverage) {
        this.cumulativeMovingAverage = cumulativeMovingAverage;
    }
    public int getCumulativeMovingAverage() {
        return cumulativeMovingAverage;
    }

    /**
     * Resets the state of the CMA
     */
    public void clear(){
        cumulativeMovingAverage = 0;
        numberOfDatumPoints = 0;
    }

    @Override
    public String toString() {
        return "CMA=" + cumulativeMovingAverage +
                ", datumPoints=" + numberOfDatumPoints;
    }

    /**
     * Method for efficient serialization
     * @param kryo
     * @param output
     */
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(cumulativeMovingAverage);
        output.writeInt(numberOfDatumPoints);
    }

    /**
     * Method for efficient desrialization
     * @param kryo
     * @param input
     */
    @Override
    public void read(Kryo kryo, Input input) {
        cumulativeMovingAverage = input.readInt();
        numberOfDatumPoints = input.readInt();
    }
}
