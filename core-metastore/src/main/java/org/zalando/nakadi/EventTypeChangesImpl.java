package org.zalando.nakadi;


import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.echocat.jomon.runtime.concurrent.Retryer;

import java.io.Closeable;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EventTypeChangesImpl implements EventTypeChanges {

    static class Change {
        private final String eventType;
        private final Instant firstSeen;
        private final int version;

        public Change(final String eventType, final Instant firstSeen, final int version) {
            this.eventType = eventType;
            this.firstSeen = firstSeen;
            this.version = version;
        }

        public Instant getFirstSeen() {
            return firstSeen;
        }

        public String getEventType() {
            return eventType;
        }

        public int getVersion() {
            return version;
        }
    }

    private final Random random = new SecureRandom();
    private final CuratorFramework curatorFramework;
    private final String cachePath;
    private final List<Consumer<String>> listeners = new ArrayList<>();
    private final HashMap<String, List<Consumer<String>>> perEventTypeListeners = new HashMap<>();

    // We are constructing ordered set of changes, so first ones will be removed.
    private final Map<String, Change> eventTypeChanges = new HashMap<>();
    private volatile boolean createWatcher = true;

    public EventTypeChangesImpl(final CuratorFramework curatorFramework, final String cachePath) {
        this.curatorFramework = curatorFramework;
        this.cachePath = cachePath;
    }

    public void run() {
        // 1. Read all the new data.
        boolean somethingChanged = false;
        boolean watcherCreated = false;
        final Set<String> newChildren;
        if (createWatcher) {
            newChildren = new HashSet<>(curatorFramework.getChildren().usingWatcher(this::childrenChanged).forPath(cachePath));
            createWatcher = false;
            watcherCreated = true;
        } else {
            newChildren = new HashSet<>(curatorFramework.getChildren().forPath(cachePath));
        }
        // First step - remove changes that were removed from zookeeper
        final Set<String> dataToRemove = eventTypeChanges.keySet().stream()
                .filter(v -> !newChildren.contains(v))
                .collect(Collectors.toSet());
        if (!dataToRemove.isEmpty()) {
            somethingChanged = true;
            dataToRemove.forEach(eventTypeChanges::remove);
        }

        final Set<Change> dataToUpdate = new HashSet<>();
        for (final String eventType : newChildren) {
            final Stat stat = new Stat();
            try {
                curatorFramework.getData().storingStatIn(stat).forPath(this.cachePath + "/" + eventType);
            } catch (KeeperException.NoNodeException ex) {
                // It's fine - while we were checking node was removed
                continue;
            }
            dataToUpdate.add(new Change(eventType, Instant.now(), stat.getVersion()));
        }
        // Now we are creating changelist.
        final Set<Change> eventTypesToNotifyAbout = new HashSet<>();
        for (final Change newChange : dataToUpdate) {
            final Change oldChange = eventTypeChanges.get(newChange.getEventType());
            if (oldChange == null) {
                eventTypesToNotifyAbout.add(newChange);
            } else {
                if (oldChange.getVersion() != newChange.getVersion()) {
                    eventTypesToNotifyAbout.add(newChange);
                }
            }
        }
        if (!eventTypesToNotifyAbout.isEmpty()) {
            somethingChanged = true;
            eventTypesToNotifyAbout.forEach(evt -> {
                if (nottifyAboutEventChange(evt.getEventType())) {
                    eventTypeChanges.put(evt.getEventType(), evt);
                }
            });
        }
        if (somethingChanged && !watcherCreated) {
            // suppose that we were not notified about changes, but were refreshing in time to time form.
            // Probably that means that our watcher disappeared. Let's recreate it.
            createWatcher = true;
            addTask(this::run);
        }
        ;
        Duration notificationsTTL = Duration.of(notificationsTTLSeconds, ChronoUnit.SECONDS);
        final Instant oldies = Instant.now().minus(notificationsTTL);
        // Clean up old changes.
        final Set<Change> toRemove = eventTypeChanges.values().stream()
                .filter(change -> change.getFirstSeen().compareTo(oldies) < 0)
                .collect(Collectors.toSet());
        for (final Change change: toRemove) {
            try {
                curatorFramework.delete().withVersion(change.getVersion()).forPath(cachePath + "/" + change.getEventType());
                eventTypeChanges.remove(et);
            } catch (KeeperException.NoNodeException ex) {
                // that's fine, someone else removed it.
                eventTypeChanges.remove(et);
            } catch (KeeperException.BadVersionException ex) {
                // That is also fine, we will get it on new update
                eventTypeChanges.remove(et);
                addTask(this::childrenChanged);
            }
        }
    }

    private void childrenChanged(final WatchedEvent evt) {
        createWatcher = true;
        attTask(this::run);
    }

    @Override
    public void registerChange(final String eventTypeName) {
        final String fullEventPath = cachePath + "/" + eventTypeName;
        Retryer.executeWithRetry(
                () -> {
                    Stat existingNodeStat = null;
                    try {
                        final Stat stat = new Stat();
                        curatorFramework.getData().storingStatIn(stat).forPath(fullEventPath);
                        existingNodeStat = stat;
                    } catch (KeeperException.NoNodeException ex) {
                        existingNodeStat = null;
                    }
                    if (existingNodeStat == null) {
                        // Node is not found. Possible outcome - node exists, success
                        curatorFramework.create()
                                .creatingParentsIfNeeded()
                                .forPath(fullEventPath, "-1".getBytes(Charsets.UTF_8));
                    } else {
                        // Node exists, we need to update it. Possible outcome - no node, bad version, success
                        curatorFramework.setData()
                                .withVersion(existingNodeStat.getVersion())
                                .forPath(
                                        fullEventPath,
                                        String.valueOf(existingNodeStat.getVersion()).getBytes(Charsets.UTF_8));
                    }
                    // Remember, that we are adding value just because we would like to avoid issues with not triggering
                    // notification for values that are not changing.
                    return null;
                },
                new RetryForSpecifiedTimeStrategy<Void>(TimeUnit.SECONDS.toMillis(10))
                        .withWaitBetweenEachTry(random.nextInt(100))
                        .withExceptionsThatForceRetry(
                                KeeperException.NodeExistsException.class,
                                KeeperException.BadVersionException.class,
                                KeeperException.NoNodeException.class)
        );
    }

    @Override
    public Closeable registerListener(final Consumer<String> listener) {
        synchronized (this.listeners) {
            this.listeners.add(listener);
        }
        return () -> {
            synchronized (this.listeners) {
                this.listeners.remove(listener);
            }
        };
    }

    @Override
    public Closeable registerListener(final String key, final Consumer<String> listener) {
        synchronized (perEventTypeListeners) {
            perEventTypeListeners.computeIfAbsent(key, (ignore) -> new ArrayList<>())
                    .add(listener);
        }
        return () -> {
            synchronized (perEventTypeListeners) {
                final List<Consumer<String>> list = perEventTypeListeners.get(key);
                if (null != list) {
                    list.remove(listener);
                    if (list.isEmpty()) {
                        perEventTypeListeners.remove(key);
                    }
                }
            }
        };
    }

}
