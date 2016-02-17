package uk.ac.gla.atanaspam.network;

import org.testng.annotations.Test;
import uk.ac.gla.atanaspam.network.utils.HitCountKeeper;
import uk.ac.gla.atanaspam.network.utils.HitCountPair;

import static org.testng.AssertJUnit.assertEquals;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

/**
 * @author atanaspam
 * @version 0.1
 * @created 09/02/2016
 */
public class HitCountKeeperTest {


    @Test
    public void shouldCalculateCMA() {
        HitCountKeeper k = new HitCountKeeper();
        InetAddress a = null;
        try {
            a = InetAddress.getByName("192.168.1.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        int j=0;
        int result = 0;
        //k.addSrcIpHitCount(a, 10);
        Random r = new Random();
        for (int i=0; i<10; i++){
            j=r.nextInt(9999)+1;
            if (i ==0) {
                result = j;
            }else {
                result = (j + (i * result)) / (i + 1);
            }
            k.addSrcIpHitCount(a, j);
            //System.out.println(j + " --- " + k.getSrcIpCMA(a) + " -- " + result);
        }
        assertEquals(result, k.getSrcIpCMA(a));
    }

    //TODO fix this to use Last10CMA instead of HitCountKeeper
//    @Test
//    public void shouldCalculateLast10CMA() {
//        HitCountKeeper k = new HitCountKeeper();
//        InetAddress a = null;
//        Random r = new Random();
//        int[] b = new int[10];
//        int n = 0;
//        try {
//            a = InetAddress.getByName("192.168.1.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        int j=0;
//        for (int i=0; i<10; i++){
//            j=r.nextInt(9999)+1;
//            b[i] = j;
//            k.addSrcIpHitCount(a, j);
//        }
//        assertEquals(getSum(b)/10, k.getSrcIpCMA(a));
//        for (int i=0; i<10; i++){
//            k.addSrcIpHitCount(a, 1000);
//        }
//
//        assertEquals(1000, k.getSrcIpCMA(a));
//    }

    public int getSum(int[]a){
        int j=0;
        for (int i=0;i<10;i++){
            j+=a[i];
        }
        return j;
    }
}
