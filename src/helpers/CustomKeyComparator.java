package helpers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Comparator;

/**
 * A custom Comparator that compares based on the type of input,
 * as the user_id can either be a numerical value, an IP address or
 * a string in the case of administrators.
 */
public class CustomKeyComparator implements Comparator<String> {
    @Override
    public int compare(String first, String second) {
        try {
            Integer firstInt = Integer.parseInt(first);
            Integer secondInt = Integer.parseInt(second);
            // Compare user id numerical values
            int cmp = firstInt.compareTo(secondInt);

            if (cmp != 0){
                return cmp;
            }

            String[] firstIpAddress = first.split(":");
            String[] secondIpAddress = second.split(":");

            InetAddress ip1 = InetAddress.getByName(firstIpAddress[1]);
            InetAddress ip2 = InetAddress.getByName(secondIpAddress[1]);

            IPAddressComparator comp = new IPAddressComparator();

            // Compare IP Addresses
            return comp.compare(ip1, ip2);
        }
        catch (NumberFormatException|UnknownHostException e) {
            System.err.println("The user_id cannot be mapped to a numerical value. It will map to" +
                    " an IP or a String." + e.getMessage
                    ());
        }
        // Compare administrators (string)
        return first.compareTo(second);
    }
}