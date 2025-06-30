/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.opensearch.repositories.oci;
import com.carrotsearch.randomizedtesting.ThreadFilter;

public class IgnoreForkJoinCommonPoolThreads implements ThreadFilter {
    @Override
    public boolean reject(Thread thread) {
        ThreadGroup group = thread.getThreadGroup();
        if(group != null && "TGRP-OciObjectStoragePluginTests".equals(group.getName()))
            return true;
        return false;
    }
}