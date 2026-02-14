package ru.digilabs.alkir.rahc.service;

import com._1c.v8.ibis.admin.client.IAgentAdminConnectorFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.digilabs.alkir.rahc.configuration.RasConfigurationProperties;
import ru.digilabs.alkir.rahc.dto.ConnectionDTO;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Пул соединений RacService с кэшированием по ключу (address:port).
 * Позволяет переиспользовать соединения между HTTP-запросами,
 * избегая накладных расходов на установление соединения и аутентификацию.
 */
@Service
@Slf4j
public class RacServicePool {

    private final ObjectProvider<RacService> racServiceObjectProvider;
    private final IAgentAdminConnectorFactory factory;

    private final Map<String, PooledRacService> pool = new ConcurrentHashMap<>();

    @Value("${rac.pool.ttl-seconds:300}")
    private long ttlSeconds;

    @Value("${rac.pool.max-size:100}")
    private int maxPoolSize;

    public RacServicePool(
        @Qualifier("racService") ObjectProvider<RacService> racServiceObjectProvider,
        IAgentAdminConnectorFactory factory
    ) {
        this.racServiceObjectProvider = racServiceObjectProvider;
        this.factory = factory;
    }

    /**
     * Получает RacService из пула или создаёт новый.
     * Возвращает обёртку PooledRacServiceWrapper, которая при close() не закрывает соединение,
     * а возвращает его в пул.
     */
    public RacService getRacService(ConnectionDTO connection) {
        var key = buildKey(connection);
        var now = Instant.now();

        var pooled = pool.compute(key, (k, existing) -> {
            if (existing != null && !existing.isExpired(now, ttlSeconds)) {
                existing.updateLastAccess(now);
                LOGGER.debug("Reusing pooled RacService for key: {}", k);
                return existing;
            }

            // Закрываем старое соединение если есть
            if (existing != null) {
                LOGGER.debug("Closing expired RacService for key: {}", k);
                existing.closeQuietly();
            }

            // Проверяем размер пула
            if (pool.size() >= maxPoolSize) {
                evictOldest();
            }

            LOGGER.debug("Creating new RacService for key: {}", k);
            var configurationProperties = connection.toConfigurationProperties();
            var racService = racServiceObjectProvider.getObject(configurationProperties, factory);
            return new PooledRacService(racService, now);
        });

        return new PooledRacServiceWrapper(pooled.getRacService());
    }

    /**
     * Удаляет соединение из пула (например, при ошибке).
     */
    public void invalidate(ConnectionDTO connection) {
        var key = buildKey(connection);
        var removed = pool.remove(key);
        if (removed != null) {
            LOGGER.debug("Invalidating RacService for key: {}", key);
            removed.closeQuietly();
        }
    }

    /**
     * Периодическая очистка устаревших соединений.
     */
    @Scheduled(fixedDelayString = "${rac.pool.cleanup-interval-ms:60000}")
    public void cleanupExpired() {
        var now = Instant.now();
        var expiredCount = 0;

        var iterator = pool.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue().isExpired(now, ttlSeconds)) {
                LOGGER.debug("Removing expired RacService for key: {}", entry.getKey());
                entry.getValue().closeQuietly();
                iterator.remove();
                expiredCount++;
            }
        }

        if (expiredCount > 0) {
            LOGGER.info("Cleaned up {} expired RacService connections. Pool size: {}", expiredCount, pool.size());
        }
    }

    /**
     * Вытесняет самое старое соединение из пула.
     */
    private void evictOldest() {
        var oldest = pool.entrySet().stream()
            .min((e1, e2) -> e1.getValue().getLastAccess().compareTo(e2.getValue().getLastAccess()))
            .orElse(null);

        if (oldest != null) {
            LOGGER.debug("Evicting oldest RacService for key: {}", oldest.getKey());
            pool.remove(oldest.getKey());
            oldest.getValue().closeQuietly();
        }
    }

    private String buildKey(ConnectionDTO connection) {
        var props = connection.toConfigurationProperties();
        return props.getAddress() + ":" + props.getPort() + ":" +
            props.getClusterAdminUsername() + ":" + props.getClusterAdminPassword();
    }

    public int getPoolSize() {
        return pool.size();
    }

    /**
     * Внутренний класс для хранения RacService с метаданными.
     */
    private static class PooledRacService {
        private final RacService racService;
        private Instant lastAccess;

        PooledRacService(RacService racService, Instant createdAt) {
            this.racService = racService;
            this.lastAccess = createdAt;
        }

        RacService getRacService() {
            return racService;
        }

        Instant getLastAccess() {
            return lastAccess;
        }

        void updateLastAccess(Instant time) {
            this.lastAccess = time;
        }

        boolean isExpired(Instant now, long ttlSeconds) {
            return lastAccess.plusSeconds(ttlSeconds).isBefore(now);
        }

        void closeQuietly() {
            try {
                racService.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing RacService", e);
            }
        }

        private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PooledRacService.class);
    }

    /**
     * Обёртка над RacService, которая при close() не закрывает реальное соединение.
     */
    private static class PooledRacServiceWrapper extends RacService {

        private final RacService delegate;

        PooledRacServiceWrapper(RacService delegate) {
            super(null, null);
            this.delegate = delegate;
        }

        @Override
        public void close() {
            // Не закрываем - соединение остаётся в пуле
        }

        @Override
        public void init() {
            // Не инициализируем - используем делегат
        }

        // Делегируем все методы к реальному RacService
        @Override
        public java.util.UUID getClusterId(ConnectionDTO connection) {
            return delegate.getClusterId(connection);
        }

        @Override
        public java.util.UUID getIbId(ConnectionDTO connection) {
            return delegate.getIbId(connection);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IClusterInfo> getClusters() {
            return delegate.getClusters();
        }

        @Override
        public com._1c.v8.ibis.admin.IClusterInfo getClusterInfo(java.util.UUID clusterId) {
            return delegate.getClusterInfo(clusterId);
        }

        @Override
        public java.util.UUID editCluster(com._1c.v8.ibis.admin.IClusterInfo clusterInfo) {
            return delegate.editCluster(clusterInfo);
        }

        @Override
        public void deleteCluster(java.util.UUID clusterId) {
            delegate.deleteCluster(clusterId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IClusterManagerInfo> getClusterManagers(java.util.UUID clusterId) {
            return delegate.getClusterManagers(clusterId);
        }

        @Override
        public com._1c.v8.ibis.admin.IClusterManagerInfo getClusterManagerInfo(java.util.UUID clusterId, java.util.UUID managerId) {
            return delegate.getClusterManagerInfo(clusterId, managerId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IClusterServiceInfo> getClusterServices(java.util.UUID clusterId) {
            return delegate.getClusterServices(clusterId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IClusterServiceInfo> getClusterServices(java.util.UUID clusterId, java.util.UUID managerId) {
            return delegate.getClusterServices(clusterId, managerId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IInfoBaseInfo> getInfoBasesFull(java.util.UUID clusterId) {
            return delegate.getInfoBasesFull(clusterId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IInfoBaseInfoShort> getInfoBases(java.util.UUID clusterId) {
            return delegate.getInfoBases(clusterId);
        }

        @Override
        public com._1c.v8.ibis.admin.IInfoBaseInfoShort getInfoBase(java.util.UUID clusterId, java.util.UUID ibId) {
            return delegate.getInfoBase(clusterId, ibId);
        }

        @Override
        public com._1c.v8.ibis.admin.IInfoBaseInfo getInfoBaseFull(java.util.UUID clusterId, java.util.UUID ibId, java.util.Optional<String> ibUsername, java.util.Optional<String> ibPassword) {
            return delegate.getInfoBaseFull(clusterId, ibId, ibUsername, ibPassword);
        }

        @Override
        public void updateInfoBase(java.util.UUID clusterId, com._1c.v8.ibis.admin.IInfoBaseInfo ibInfo, java.util.Optional<String> ibUsername, java.util.Optional<String> ibPassword) {
            delegate.updateInfoBase(clusterId, ibInfo, ibUsername, ibPassword);
        }

        @Override
        public java.util.UUID createInfoBase(java.util.UUID clusterId, com._1c.v8.ibis.admin.IInfoBaseInfo ibInfo, int mode) {
            return delegate.createInfoBase(clusterId, ibInfo, mode);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.ISessionInfo> getSessions(java.util.UUID clusterId) {
            return delegate.getSessions(clusterId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.ISessionInfo> getSessions(java.util.UUID clusterId, java.util.UUID ibId) {
            return delegate.getSessions(clusterId, ibId);
        }

        @Override
        public com._1c.v8.ibis.admin.ISessionInfo getSessionInfo(java.util.UUID clusterId, java.util.UUID sid) {
            return delegate.getSessionInfo(clusterId, sid);
        }

        @Override
        public void terminateSession(java.util.UUID clusterId, java.util.UUID sid, java.util.Optional<String> message) {
            delegate.terminateSession(clusterId, sid, message);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IWorkingServerInfo> getWorkingServers(java.util.UUID clusterId) {
            return delegate.getWorkingServers(clusterId);
        }

        @Override
        public com._1c.v8.ibis.admin.IWorkingServerInfo getWorkingServer(java.util.UUID clusterId, java.util.UUID serverId) {
            return delegate.getWorkingServer(clusterId, serverId);
        }

        @Override
        public java.util.UUID editWorkingServer(java.util.UUID clusterId, com._1c.v8.ibis.admin.IWorkingServerInfo serverInfo) {
            return delegate.editWorkingServer(clusterId, serverInfo);
        }

        @Override
        public void deleteWorkingServer(java.util.UUID clusterId, java.util.UUID serverId) {
            delegate.deleteWorkingServer(clusterId, serverId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IWorkingProcessInfo> getWorkingProcesses(java.util.UUID clusterId) {
            return delegate.getWorkingProcesses(clusterId);
        }

        @Override
        public com._1c.v8.ibis.admin.IWorkingProcessInfo getWorkingProcess(java.util.UUID clusterId, java.util.UUID processId) {
            return delegate.getWorkingProcess(clusterId, processId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IWorkingProcessInfo> getServerWorkingProcesses(java.util.UUID clusterId, java.util.UUID serverId) {
            return delegate.getServerWorkingProcesses(clusterId, serverId);
        }

        @Override
        public java.util.List<com._1c.v8.ibis.admin.IClusterManagerInfo> getServerClusterManagers(java.util.UUID clusterId, java.util.UUID serverId) {
            return delegate.getServerClusterManagers(clusterId, serverId);
        }
    }
}