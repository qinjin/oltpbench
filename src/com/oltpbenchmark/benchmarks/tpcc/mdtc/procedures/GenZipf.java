package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.distribution.ZipfDistribution;

import com.google.common.collect.Queues;
import com.google.common.io.Files;

public class GenZipf {
    private static final LinkedBlockingQueue<Integer> zipfID = Queues.newLinkedBlockingQueue();

    public static void main(String[] args) throws Exception {
        initZipf(1d, 100000, 30000);
    }

    public static void initZipf(double zipfExponent, int zipfKeyspace, int zipfQueueSize) throws Exception {
        System.out.println("Started to init zipf with Exponent="+zipfExponent+" Keyspace=" + zipfKeyspace + " ResultSize=" + zipfQueueSize + "...");
        initByMultiThreads(zipfExponent, 10, zipfKeyspace, zipfQueueSize);

        File file = new File("/home/qinjin/mdtc/oltpbench/zipf_100000.txt");
        if(file.exists()){
            file.delete();
        }
        file.createNewFile();
        Iterator<Integer> iter = zipfID.iterator();
        boolean first = true;
        while (iter.hasNext()) {
            Integer id = iter.next();
            String str = "";
            if (first) {
                str = String.valueOf(id);
                first = false;
            } else {
                str = "," + id;
            }
            Files.append(str, file, Charset.defaultCharset());
        }

        System.out.println("Saved  zipf values to file " + file);
    }

    private static void initByMultiThreads(final double zipfExponent, final int numThread, final int zipfKeyspace, final int zipfQueueSize) {
        final ZipfDistribution zipf = new ZipfDistribution(zipfKeyspace, zipfExponent);
        final CountDownLatch countDownLatch = new CountDownLatch(numThread);
        for (int i = 0; i < numThread; i++) {
            Runnable r = new Runnable() {

                @Override
                public void run() {
                    long start = System.currentTimeMillis();
                    for (int j = 0; j < zipfQueueSize / numThread; j++) {
                        zipfID.add(zipf.sample());
                        if (j % 200 == 0) {
                            System.out.println("Thread " + Thread.currentThread().getId() + " Generate " + j + " zipf took " + (System.currentTimeMillis() - start) + " ms.");
                        }
                    }

                    countDownLatch.countDown();
                }
            };
            Thread t = new Thread(r);
            t.start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
//    private static void initByOneThread(double zipfExponent, long startMs) {
//        final ZipfDistribution zipf = new ZipfDistribution(ZIPF_KEY_SPACE, zipfExponent);
//        for (int i = 0; i < ZIPF_KEY_SPACE; i++) {
//            if (i % 100 == 0) {
//                System.out.println("Generate " + i + " zipf took " + (System.currentTimeMillis() - startMs) + " ms.");
//            }
//            zipfOID.add(zipf.sample());
//        }
//    }
}
