/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.test;

import org.lealone.docdb.main.LealoneDocDB;

public class DocDBServerStart {

    public static void main(String[] args) {
        LealoneDocDB.main(args, "docdb-test.yaml");
    }

}
