package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.time.Duration

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
    private lateinit var rateLimiter: RateLimiter

    @PostConstruct
    private fun initialize() {
        paymentExecutor = ThreadPoolExecutor(
            150,
            150,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(10_000),
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
        paymentExecutor.prestartAllCoreThreads()
        executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())
        rateLimiter =
//            LeakingBucketRateLimiter(
//                rate = 1000,
//                window = Duration.ofMillis(500),
//                bucketSize = 5000
//            )
            TokenBucketRateLimiter(
                rate = 1600,
                window = 1,
                bucketMaxCapacity = 1000,
                timeUnit = TimeUnit.SECONDS
            )
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (!rateLimiter.tick()) {
            throw TooManyRequestsError(50)
        }

        executorScope.launch {
            try {
                val createdEvent = withContext(Dispatchers.IO) {
                    paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
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