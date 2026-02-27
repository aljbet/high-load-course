package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

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
//    private var averageProcessingTime: Long = 10
//    private var rateLimitPerSec: Int = 5000
//    private var parallelRequests: Int = 2000

//    private lateinit var rateLimiter: RateLimiter
    private lateinit var orderPayerQueueMetric: Counter
    private lateinit var orderPayerAfterRlQueueMetric: Counter
    private lateinit var counterMetricBeforeTick: Counter
    private lateinit var counterMetricAfterTick: Counter
    private lateinit var counterMetricAfterJob: Counter
    private val pending = AtomicLong(0)

    @PostConstruct
    private fun initialize() {
        paymentExecutor = ThreadPoolExecutor(
            16,
            16,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(),
            NamedThreadFactory("payment-submission-executor"),
//            CallerBlockingRejectedExecutionHandler()
            ThreadPoolExecutor.AbortPolicy()
        )
        executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())
//        rateLimiter =
//            TokenBucketRateLimiter(
//                rate = 800,
//                bucketMaxCapacity = 560,
//                window = 1000,
//            )
        counterMetricBeforeTick = Counter.builder("MY_METR_to_order_payer_before_tick").register(promRegistry)
        counterMetricAfterTick = Counter.builder("MY_METR_to_order_payer_after_tick").register(promRegistry)
        counterMetricAfterJob = Counter.builder("MY_METR_after_job").register(promRegistry)
        orderPayerQueueMetric = Counter.builder("order_payer_queue").register(promRegistry)
        orderPayerAfterRlQueueMetric = Counter.builder("order_payer_after_rl_queue").register(promRegistry)
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        counterMetricBeforeTick.increment()
        orderPayerQueueMetric.increment()
        val createdAt = System.currentTimeMillis()
        val rateSec = 5000.0 / 60
        val currentPending = pending.getAndIncrement()

//        if (!rateLimiter.tick()) {
//            throw TooManyRequestsError(50)
//        }
        if (currentPending >= rateSec * 1.0) {
            pending.decrementAndGet()
            val millisToRetry = ((currentPending / rateSec) * 1000).toLong() + 100
            throw TooManyRequestsError(millisToRetry)
        }
        orderPayerAfterRlQueueMetric.increment()

//        executorScope.async {
        executorScope.launch {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline) {
                    pending.decrementAndGet()
                }
            } catch (e: Exception) {
                logger.error("Failed to process payment for order $orderId", e)
                pending.decrementAndGet()
            }
        }
        counterMetricAfterJob.increment()
        return createdAt
    }
}

class TooManyRequestsError(val millisToRetry: Long) : RuntimeException()