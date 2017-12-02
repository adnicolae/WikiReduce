package helpers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Comparator;

public class CustomKeyComparator implements Comparator<String> {
    @Override
    public int compare(String first, String second) {
        try {
            Integer firstInt = Integer.parseInt(first);
            Integer secondInt = Integer.parseInt(second);
            int cmp = firstInt.compareTo(secondInt);

            if (cmp != 0){
                return cmp;
            }

            String[] firstIpAddress = first.split(":");
            String[] secondIpAddress = second.split(":");

            InetAddress ip1 = InetAddress.getByName(firstIpAddress[1]);
            InetAddress ip2 = InetAddress.getByName(secondIpAddress[1]);

            IPAddressComparator comp = new IPAddressComparator();

            return comp.compare(ip1, ip2);
        }
        catch (NumberFormatException|UnknownHostException e) {
            System.err.println("The user_id cannot be mapped to a numerical or ip " +
                    "value: " + e.getMessage());
        }

        return first.compareTo(second);
    }
}