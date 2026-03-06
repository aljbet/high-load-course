package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var promRegistry: MeterRegistry

    private lateinit var paymentExecutor: ThreadPoolExecutor
    private lateinit var executorScope: CoroutineScope

    @PostConstruct
    private fun initialize() {
        paymentExecutor = ThreadPoolExecutor(
            16,
            16,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(83),
            NamedThreadFactory("payment-submission-executor"),
            ThreadPoolExecutor.AbortPolicy()
        )
        executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (paymentExecutor.queue.remainingCapacity() == 0) {
            throw TooManyRequestsError(50)
        }

        executorScope.async {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (e: Exception) {
                logger.error("Failed to process payment for order $orderId", e)
            }
        }
        return createdAt
    }
}

class TooManyRequestsError(val millisToRetry: Long) : RuntimeException()