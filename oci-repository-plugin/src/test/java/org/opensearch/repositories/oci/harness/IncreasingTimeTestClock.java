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
package org.opensearch.repositories.oci.harness;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Log4j2
public class IncreasingTimeTestClock extends Clock {
    @Getter private final List<Instant> instantsVended = new ArrayList<>();

    @Setter @Getter private long increaseWithEachCall = 0;

    public void setCurrentTime(long millis) {
        this.currentTime = millis;
    }

    public Instant getLastVendedInstance() {
        return instantsVended.get(instantsVended.size() - 1);
    }

    @Override
    public Instant instant() {
        final Instant instant = Instant.ofEpochMilli(getCurrentTime());
        instantsVended.add(instant);
        tickToNewTime(getCurrentTime() + increaseWithEachCall);
        return instant;
    }

    @Getter long currentTime = 0;

    @Override
    public ZoneId getZone() {
        return null;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return null;
    }

    public void tickToNewTime(long millis) {
        if (millis >= currentTime) {
            currentTime = millis;
        } else {
            log.warn(
                    "we are trying to set time backwards will not be performing tick, current time:{}, new time: {}",
                    currentTime,
                    millis);
        }
    }
}
