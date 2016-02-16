package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.Arrays;

/**
 * @author atanaspam
 * @version 0.1
 * @created 09/02/2016
 */
public class CMAPair implements KryoSerializable{

    int cumulativeMovingAverage;
    int numberOfDatumPoints;

    public CMAPair(int cumulativeMovingAverage, int numberOfDatumPoints) {
        this.cumulativeMovingAverage = cumulativeMovingAverage;
        this.numberOfDatumPoints = numberOfDatumPoints;
    }
    public CMAPair(){
        this.cumulativeMovingAverage = 0;
        this.numberOfDatumPoints = 0;
    }

    /**
     * Adds a new value to the cumulative Moving Average until now
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

    public void clear(){
        cumulativeMovingAverage = 0;
        numberOfDatumPoints = 0;
    }

    @Override
    public String toString() {
        return "CMA=" + cumulativeMovingAverage +
                ", datumPoints=" + numberOfDatumPoints;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(cumulativeMovingAverage);
        output.writeInt(numberOfDatumPoints);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        cumulativeMovingAverage = input.readInt();
        numberOfDatumPoints = input.readInt();
    }
}


//public class CMAPair implements KryoSerializable{
//
//    int cumulativeMovingAverage;
//    int[] oldValues;
//    int index;
//
//    public CMAPair(int value, int numberOfDatumPoints) {
//        index = 0;
//        this.oldValues = new int[10];
//        oldValues[index++] = value;
//        this.cumulativeMovingAverage = value;
//    }
//    public CMAPair(){
//        index =0;
//        this.cumulativeMovingAverage = 0;
//        this.oldValues = new int[10];
//    }
//
//    /**
//     * Adds a new value to the cumulative Moving Average until now
//     * @param value the new value to be added
//     * @return true if the new value is larger than the CMA before the update, false otherwise
//     */
//    public void addValue(int value) {
//
//        //System.out.println(index);
//        cumulativeMovingAverage = (cumulativeMovingAverage - oldValues[index]) + value;
//        //System.out.println(oldValues[index]);
//        oldValues[index] = value;
//        //System.out.println("-----"+oldValues[index]);
//        index++;
//        if (index == oldValues.length){
//            index = 0;
//        }
//    }
//
//    public void setCumulativeMovingAverage(int cumulativeMovingAverage) {
//        this.cumulativeMovingAverage = cumulativeMovingAverage;
//    }
//    public int getCumulativeMovingAverage() {
//        return cumulativeMovingAverage/10;
//    }
//
//    public void clear(){
//        cumulativeMovingAverage = 0;
//         for (int i=0; i< oldValues.length; i++){
//             oldValues[i] = 0;
//         }
//    }
//
//    @Override
//    public String toString() {
//        return "CMA=" + cumulativeMovingAverage/10;
//    }
//
//    @Override
//    public void write(Kryo kryo, Output output) {
//        output.writeInt(cumulativeMovingAverage);
//        kryo.writeClassAndObject(output, oldValues);
//    }
//
//    @Override
//    public void read(Kryo kryo, Input input) {
//        cumulativeMovingAverage = input.readInt();
//        oldValues = (int[]) kryo.readClassAndObject(input);
//    }
//}
