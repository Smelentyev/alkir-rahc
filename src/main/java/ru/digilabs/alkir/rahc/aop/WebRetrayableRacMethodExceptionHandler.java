package ru.digilabs.alkir.rahc.aop;

import lombok.SneakyThrows;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("web")
public class WebRetrayableRacMethodExceptionHandler implements RetrayableRacMethodExceptionHandler {
    @Override
    @SneakyThrows
    public void handle(Throwable throwable) {
        throw throwable;
    }
}
