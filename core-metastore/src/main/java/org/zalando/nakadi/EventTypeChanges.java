package org.zalando.nakadi;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * This interface is used to propagate changes in event types between all the instances of nakadi.
 * Instead of using complex caching approach, it tries to propagate only small changeset,
 */
public interface EventTypeChanges {
    // Registers change, that will be propagated across all the instances
    void registerChange(String eventTypeName);

    // Register listener, that will react on the changes from all the instances
    Closeable registerListener(Consumer<String> listener);

    // Register listener, that will react on the changes from all the instances, but only for specific keys
    Closeable registerListener(String key, Consumer<String> listener);
}
