package helpers;

import java.net.InetAddress;
import java.util.Comparator;

/**
 * A custom Comparator that compares IP addresses based on the bytes
 * separated by dots. Only IP addresses that are of the same type
 * (same number of bytes in each field) are sorted.
 */
public class IPAddressComparator implements Comparator<InetAddress> {
    @Override
    public int compare(InetAddress adr1, InetAddress adr2) {
        byte[] ba1 = adr1.getAddress();
        byte[] ba2 = adr2.getAddress();

        // Order based on length
        if(ba1.length < ba2.length) return -1;
        if(ba1.length > ba2.length) return 1;

        // Compare each byte of the two IPs if they are of the same length
        for(int i = 0; i < ba1.length; i++) {
            int b1 = Byte.toUnsignedInt(ba1[i]);
            int b2 = Byte.toUnsignedInt(ba2[i]);
            if(b1 == b2)
                continue;
            if(b1 < b2)
                return -1;
            else
                return 1;
        }
        return 0;
    }
}