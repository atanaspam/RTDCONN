package uk.ac.gla.atanaspam.network.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This pair stores hitCount and the iteration during which it was last incremented.
 * @author atanaspam
 * @version 0.1
 * @created 07/02/2016
 */
public class HitCountPair implements KryoSerializable {
    int hitCount;
    int iterationNumber;

    public HitCountPair(){
        this.hitCount = 0;
        this.iterationNumber = -2;
    }

    public HitCountPair(int hitCount, int iterationNumber) {
        this.hitCount = hitCount;
        this.iterationNumber = iterationNumber;
    }

    /**
     * Increments the hit count for that taskId and sets the iterationNumber to the current value
     * @param iterationNumber the current IterationNumber
     * @return true if the value has been incremented during the last iteration, false otherwise
     */
    public boolean increment(int iterationNumber){
        hitCount++;
        if (this.iterationNumber == iterationNumber-1) {
            this.iterationNumber = iterationNumber;
            return true;
        }else{
            this.iterationNumber = iterationNumber;
            return false;
        }
    }

    public String toString(){
        return hitCount+"";
    }

    public String extendedtoString(){
        return "Hits:" + hitCount + " Iteration: "+ iterationNumber;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(hitCount);
        output.writeInt(iterationNumber);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        hitCount = input.readInt();
        iterationNumber = input.readInt();
    }
}
