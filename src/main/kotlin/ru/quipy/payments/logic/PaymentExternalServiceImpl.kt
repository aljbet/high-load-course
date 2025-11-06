package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.prometheus.metrics.core.metrics.Summary
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val price = properties.price
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests, true)

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofMillis(1000))
        .readTimeout(Duration.ofMillis(1000))
        .writeTimeout(Duration.ofMillis(1000))
        .build()

    private val requestLatency = Summary.builder()
        .name("request_latency")
        .help("Request latency.")
        .quantile(0.5, 0.01)
        .quantile(0.8, 0.005)
        .quantile(0.99, 0.005)
        .labelNames("status_code")
        .register()


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId, amount: $amount")
        semaphore.acquire()
        try {
            val maxRetries = 1
            val avgProcMs = requestAverageProcessingTime.toMillis()

            for (attempt in 0..maxRetries) {
                val isBeneficial = amount > price * (attempt + 1)
                rateLimiter.tickBlocking()

                val request = Request.Builder()
                    .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .post(emptyBody)
                    .build()

                try {
                    client.newCall(request).execute().use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.code} reason=${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                        requestLatency
                            .labelValues(response.code.toString())
                            .observe(response.receivedResponseAtMillis.toDouble())

                        val success = (response.isSuccessful && body.result)

                        if (success) {
                            logger.warn("[$accountName] [OK] txId=$transactionId payment=$paymentId")
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                            break
                        } else {
                            val code = response.code
                            val timeLeft = deadline - now()
                            val retriableCode = code == 200 || code == 500 || code == 502 || code == 503 || code == 504

                            if (retriableCode && attempt < maxRetries && timeLeft > avgProcMs && isBeneficial) {
                                val retryAfterMs = (500L * (1L shl attempt))
                                val maxSleep = (deadline - now() - avgProcMs).coerceAtLeast(0)
                                val sleepMs = retryAfterMs.coerceAtMost(maxSleep)

                                logger.warn("[$accountName] [RETRY] txId=$transactionId payment=$paymentId will retry after $sleepMs ms.")
                                if (sleepMs > 0) {
                                    Thread.sleep(sleepMs)
                                    continue
                                }
                            }

                            logger.warn("[$accountName] [FAIL] txId=$transactionId payment=$paymentId code=$code msg=${body.message}")
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = body.message ?: "Failed")
                            }
                            break
                        }
                    }
                } catch (e: SocketTimeoutException) {
                    val timeLeft = deadline - now()

                    if (attempt < maxRetries && timeLeft > avgProcMs + 500 && isBeneficial) {
                        Thread.sleep(500L * (1L shl attempt))
                        continue
                    } else {
                        logger.error("[$accountName] [TIMEOUT] txId=$transactionId payment=$paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        break
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    break
                }
            }
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun getProperties(): PaymentAccountProperties = properties

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()