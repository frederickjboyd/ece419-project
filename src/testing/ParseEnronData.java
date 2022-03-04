package testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper functions to deal with Enron email dataset.
 */
public class ParseEnronData {
    /**
     * Iterate through all emails and return them as a map.
     * 
     * @param dataDir Root directory of email data
     * @return Map with directory + filename as the key, email contents as value
     */
    public static Map<String, String> getEnronData(String dataDir) {
        final File ROOT_DIR = new File(dataDir);
        Map<String, String> emailMap = new HashMap<String, String>();

        for (File user : ROOT_DIR.listFiles()) {
            for (File emailFolder : user.listFiles()) {
                for (File email : emailFolder.listFiles()) {
                    if (!email.isFile()) {
                        System.out.println(
                                String.format("Should only encounter files here: %s/%s/%s", user, emailFolder, email));
                        System.out.println("Skipping...");
                        continue;
                    }

                    try {
                        Reader fReader = new FileReader(email);
                        BufferedReader br = new BufferedReader(fReader);
                        String line;
                        String buff = "";

                        // Read file line by line and add to string
                        while ((line = br.readLine()) != null) {
                            buff = buff.concat(line);
                        }

                        String key = String.format("%s/%s/%s", user.toString(), emailFolder.toString(),
                                email.toString());
                        emailMap.put(key, buff);

                        br.close();
                    } catch (FileNotFoundException fe) {
                        fe.printStackTrace();
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                }
            }
        }

        return emailMap;
    }
}
