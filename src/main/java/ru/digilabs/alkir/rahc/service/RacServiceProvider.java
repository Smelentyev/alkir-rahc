package ru.digilabs.alkir.rahc.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.digilabs.alkir.rahc.configuration.RetryableRacMethod;
import ru.digilabs.alkir.rahc.dto.ConnectionDTO;

@Service
@RequiredArgsConstructor
public class RacServiceProvider {

    private final RacServicePool racServicePool;

    @RetryableRacMethod
    public RacService getRacService(ConnectionDTO rasConnection) {
        return racServicePool.getRacService(rasConnection);
    }

    /**
     * Инвалидирует соединение в пуле (например, при ошибке).
     */
    public void invalidate(ConnectionDTO rasConnection) {
        racServicePool.invalidate(rasConnection);
    }
}
