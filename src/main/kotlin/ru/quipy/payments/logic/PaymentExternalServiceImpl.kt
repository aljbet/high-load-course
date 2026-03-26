package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentEventWriter: PaymentEventWriter,
    promRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val avgProcMs = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val price = properties.price
    private val retryAfter = 100L
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )
    private val semaphore = Semaphore(parallelRequests)
    private val circuitBreaker =
        CircuitBreakerRegistry.of(
            CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize(5)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .minimumNumberOfCalls(50)
                .build()
        ).circuitBreaker("abas")

    private val httpExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(10_000),
        NamedThreadFactory("http-executor"),
        CallerBlockingRejectedExecutionHandler()
    )
    private val httpExecutorScope = CoroutineScope(httpExecutor.asCoroutineDispatcher())

    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(2000))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val requestLatency = DistributionSummary
        .builder("request_latency")
        .publishPercentiles(0.95)
        .register(promRegistry)

    private val retryCounterMetric: Counter = Counter.builder("retry_counter").register(promRegistry)
    private val hedgeDelayMs = 175L
    private val scheduler = Executors.newScheduledThreadPool(100)
    private val maxRetries = 3

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val uri =
            URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentEventWriter.submit(paymentId) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId, amount: $amount")
        val start = now()

        suspend fun attemptCall(attempt: Int) {
            val isBeneficial = amount > price * (attempt + 1)
            semaphore.withPermit {
                rateLimiter.tickBlocking()
                try {
                    val response = sendHedge(uri)
                    val code = response.statusCode()
                    val body = try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    if (code in 200..299 && body.result) {
                        logger.warn("[$accountName] [OK] txId=$transactionId payment=$paymentId")
                        paymentEventWriter.submit(paymentId) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                        }
                        requestLatency.record((now() - start).toDouble())
                        return
                    }

                    val error = code == 500 || code == 502 || code == 503 || code == 504
                    if (error && circuitBreaker.tryAcquirePermission())

                    if (
                        (code == 200 || error) &&
                        attempt < maxRetries &&
                        (deadline - now()) > avgProcMs &&
                        isBeneficial
                    ) {
                        retryCounterMetric.increment()
                        val retryAfterMs = (retryAfter * (1L shl attempt))
                        logger.warn("[$accountName] [RETRY] txId=$transactionId payment=$paymentId will retry after $retryAfterMs ms.")
                        scheduler.schedule(
                            { httpExecutorScope.launch { attemptCall(attempt + 1) } },
                            retryAfterMs,
                            TimeUnit.MILLISECONDS
                        )
                        requestLatency.record((now() - start).toDouble())
                        return
                    }

                    logger.warn("[$accountName] [FAIL] txId=$transactionId payment=$paymentId code=$code msg=${body.message}")
                    paymentEventWriter.submit(paymentId) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = body.message ?: "Failed")
                        }
                    }
                    requestLatency.record((now() - start).toDouble())
                } catch (ex: Exception) {
                    if (
                        ex is SocketTimeoutException &&
                        attempt < maxRetries &&
                        deadline - now() > avgProcMs + retryAfter &&
                        isBeneficial
                    ) {
                        retryCounterMetric.increment()
                        val retryAfterMs = (retryAfter * (1L shl attempt))
                        scheduler.schedule(
                            { httpExecutorScope.launch { attemptCall(attempt + 1) } },
                            retryAfterMs,
                            TimeUnit.MILLISECONDS
                        )
                        return
                    }

                    logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId", ex)
                    paymentEventWriter.submit(paymentId) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = ex.message)
                        }
                    }
                }
            }
        }

        httpExecutorScope.launch { attemptCall(0) }
    }

    suspend fun sendHedge(uri: URI): HttpResponse<String> {
        val idempotencyKey = UUID.randomUUID().toString()
        val firstDeferred = send(uri, idempotencyKey)
        val allDeferred = mutableListOf(firstDeferred)
        for (hedgeIndex in 1..3) {
            val result = withTimeoutOrNull(hedgeDelayMs) {
                select {
                    allDeferred.forEach { deferred ->
                        deferred.onAwait { response ->
                            val type = if (deferred === firstDeferred) "first" else "hedge#$hedgeIndex"
                            logger.info("[HEDGE] completed from $type")
                            response
                        }
                    }
                }
            }
            if (result != null) {
                return result
            }

            if (rateLimiter.tick()) {
                val newHedge = send(uri, idempotencyKey)
                allDeferred.add(newHedge)
                logger.info("[HEDGE] added hedge #$hedgeIndex")
            } else {
                logger.info("[HEDGE] rate limiter blocked hedge #$hedgeIndex")
                continue
            }
        }
        return select {
            allDeferred.forEach { deferred ->
                deferred.onAwait { response -> response }
            }
        }
    }

    fun send(uri: URI, idempotencyKey: String): Deferred<HttpResponse<String>> {
        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofMillis(1500))   // read timeout
            .header("x-idempotency-key", idempotencyKey)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).asDeferred()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun getProperties(): PaymentAccountProperties = properties

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()