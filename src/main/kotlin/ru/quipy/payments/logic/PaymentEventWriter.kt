package ru.quipy.payments.logic

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Component
class PaymentEventWriter() {
    private val mutexes = ConcurrentHashMap<UUID, Mutex>()

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun submit(paymentId: UUID, block: () -> Unit) {
        val mutex = mutexes.computeIfAbsent(paymentId) { Mutex() }

        mutex.withLock {
            try {
                block()
            } catch (e: Exception) {
                logger.error("write failed for $paymentId", e)
            }
        }
    }
}