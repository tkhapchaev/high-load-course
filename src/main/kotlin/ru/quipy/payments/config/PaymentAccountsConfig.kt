package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.coyote.http2.Http2Protocol
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val javaClient = HttpClient.newBuilder().build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    private val allowedAccounts = setOf("acc-12")

    @Bean
    fun accountAdapters(paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=onlineStore")) // todo sukhoa service name
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        return mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter {
                it.accountName in allowedAccounts
            }.onEach(::println)
            .map { PaymentExternalSystemAdapterImpl(it, paymentService) }
    }

    @Bean
    fun tomcatConnectorCustomizer(): TomcatConnectorCustomizer {
        return TomcatConnectorCustomizer {
            try {
                (it.protocolHandler.findUpgradeProtocols().get(0) as Http2Protocol).maxConcurrentStreams = 10_000_000
            } catch (e: Exception) {
                logger.error("(!) Failed to increase number of HTTP2 streams per connection")
            }
        }
    }
}