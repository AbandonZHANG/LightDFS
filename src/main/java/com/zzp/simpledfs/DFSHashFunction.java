package main.java.com.zzp.simpledfs;
//package main.java.com.zzp.simpledfs;
/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 * —— HBase - MurmurHash3.java
 *
 * MurmurHash3 is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup.  See http://code.google.com/p/smhasher/wiki/MurmurHash3 for details.
 *
 * MurmurHash3 is the successor to MurmurHash2. It comes in 3 variants, and
 * the 32-bit version targets low latency for hash table use.
*/
public class DFSHashFunction {
//    private static DFSHashFunction _instance = new DFSHashFunction();
//
//    public static DFSHashFunction getInstance() {
//        return _instance;
//    }
    public static int hash(String key){
        byte[] keyBytes = key.getBytes();
        return hash(keyBytes, 0, keyBytes.length, 0);
    }
    public static int hash(byte[] bytes, int offset, int length, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (length & 0xfffffffc); // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (bytes[i] & 0xff) | ((bytes[i + 1] & 0xff) << 8) | ((bytes[i + 2] & 0xff) << 16)
                    | (bytes[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 >> 17); // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 >> 19); // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (length & 0x03) {
            case 3:
                k1 = (bytes[roundedEnd + 2] & 0xff) << 16;
                // FindBugs SF_SWITCH_FALLTHROUGH
            case 2:
                k1 |= (bytes[roundedEnd + 1] & 0xff) << 8;
                // FindBugs SF_SWITCH_FALLTHROUGH
            case 1:
                k1 |= (bytes[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = (k1 >> 17); // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= length;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
