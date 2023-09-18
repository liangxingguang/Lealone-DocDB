/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.main;

import org.lealone.main.Lealone;
import org.lealone.main.config.Config;

public class LealoneDocDB {

    public static void main(String[] args) {
        main(args, "docdb.yaml");
    }

    public static void main(String[] args, String configFile) {
        String cf = configFile;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i].trim();
            if (arg.equals("-config")) {
                cf = args[++i];
                break;
            }
        }
        Config.setProperty("config", cf);
        Lealone.main(args);
    }
}
