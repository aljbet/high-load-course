package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.UUID


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    @Autowired
    private lateinit var promRegistry: MeterRegistry

    private lateinit var paymentSystemQueueMetric: Counter

    @PostConstruct
    private fun initialize() {
        paymentSystemQueueMetric = Counter.builder("payment_system_queue").register(promRegistry)
    }

    override suspend fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, onComplete: () -> Unit) {
        for (account in paymentAccounts) {
            paymentSystemQueueMetric.increment()
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, onComplete)
        }
    }

    override fun getAllAccountProperties(): List<PaymentAccountProperties> = paymentAccounts.map { it.getProperties() }
}