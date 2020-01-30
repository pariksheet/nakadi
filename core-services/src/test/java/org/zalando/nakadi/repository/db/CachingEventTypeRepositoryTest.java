package org.zalando.nakadi.repository.db;

import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.repository.EventTypeRepository;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CachingEventTypeRepositoryTest {

    private final EventTypeRepository dbRepo = mock(EventTypeRepository.class);
    private final EventTypeCache cache = mock(EventTypeCache.class);
    private final EventTypeRepository cachedRepo;
    private final EventType et = mock(EventType.class);

    public CachingEventTypeRepositoryTest() {
        this.cachedRepo = new CachingEventTypeRepository(dbRepo, cache);

        Mockito
                .doReturn("event-name")
                .when(et)
                .getName();
    }

    @Test
    public void whenDbPersistenceSucceedThenNotifyCache() {
        cachedRepo.saveEventType(et);

        verify(dbRepo, times(1)).saveEventType(et);
    }

    @Test
    public void whenCacheFailsThenRollbackEventPersistence() {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .updated("event-name");

        try {
            cachedRepo.saveEventType(et);
        } catch (InternalNakadiException e) {
            verify(dbRepo, times(1)).removeEventType("event-name");
        }
    }

    @Test
    public void whenDbUpdateSucceedThenNotifyCache() {
        cachedRepo.update(et);

        verify(dbRepo, times(1)).update(et);
    }

    @Test
    public void whenUpdateCacheFailThenRollbackDbPersistence() {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .updated("event-name");

        final EventType original = mock(EventType.class);

        Mockito
                .doReturn(original)
                .when(dbRepo)
                .findByName("event-name");

        try {
            cachedRepo.update(et);
        } catch (InternalNakadiException e) {
            verify(dbRepo, times(1)).update(original);
        }
    }

    @Test
    public void removeFromDbSucceedNotifyCache() {
        cachedRepo.removeEventType("event-name");

        verify(dbRepo, times(1)).removeEventType("event-name");
    }

    @Test
    public void whenRemoveCacheEntryFailsThenRollbackDbRemoval() {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .removed("event-name");

        final EventType original = mock(EventType.class);

        Mockito
                .doReturn(original)
                .when(dbRepo)
                .findByName("event-name");

        try {
            cachedRepo.removeEventType("event-name");
        } catch (InternalNakadiException e) {
            verify(dbRepo, times(1)).saveEventType(original);
        }
    }
}
