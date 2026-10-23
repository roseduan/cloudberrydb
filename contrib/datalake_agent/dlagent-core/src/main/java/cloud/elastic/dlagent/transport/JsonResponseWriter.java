/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.service.ServiceResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;

/**
 * The single serialization exit.
 *
 * <p>Both transports turn a {@link ServiceResult} into response bytes through
 * this one class, holding the one {@link ObjectMapper}. That is what makes
 * "the two transports hand the C side identical bytes" a structural property
 * instead of two configurations that have to be kept in sync by hand -- the failure mode being a Boot upgrade that
 * shifts one Jackson default and silently splits the two paths on dates,
 * nulls or field order, which no semantic test can see.
 *
 * <p>Consequently: dlagent-grpc must not construct an ObjectMapper of its own,
 * and dlagent-rest must not keep a second serialization path.
 */
@Named
@Singleton
public class JsonResponseWriter {

    private static final byte[] NO_BODY = new byte[0];

    private final ObjectMapper mapper;

    @Inject
    public JsonResponseWriter() {
        this(DlObjectMapperFactory.create());
    }

    /** Visible for tests that need to diff against a differently-built mapper. */
    JsonResponseWriter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * The one mapper. Exposed so each composition root can install this exact
     * instance into its transport (in dlagent-rest, into Spring's
     * MappingJackson2HttpMessageConverter) rather than letting the transport
     * build a second one.
     */
    public ObjectMapper objectMapper() {
        return mapper;
    }

    /**
     * The response body for a result, as bytes.
     *
     * <p>Three cases, in this order, and the order is load-bearing:
     *
     * <ol>
     * <li>No body at all -- an empty array. A successful result carrying no
     *     data is today's {@code ResponseEntity.ok().build()} (for example
     *     {@code tableExists} on the success path), which sends zero bytes.
     *     Serializing null instead would put the four bytes {@code null} on
     *     the wire and change the C side's parse.
     * <li>Already-serialized body -- returned verbatim. {@code byte[]} and
     *     {@code String} bodies are JSON documents the service layer produced
     *     itself ({@code loadMetadataJson} returns {@code byte[] doc}), and
     *     today Spring writes them raw. Handing a {@code byte[]} to Jackson
     *     would Base64-encode it and a {@code String} would come back
     *     quote-wrapped and escaped -- both silent corruptions, both invisible
     *     to a test that only checks the HTTP status.
     * <li>Anything else -- serialized by the one mapper.
     * </ol>
     *
     * <p>Failures on the last case are wrapped: a response that cannot be
     * serialized is an internal fault, not something a caller can correct.
     */
    public byte[] write(ServiceResult<?> result) {
        Object body = result.isSuccess() ? result.getData() : result.getErrorResponse();
        return writeBody(body);
    }

    /**
     * As {@link #write(ServiceResult)} but for a bare value, for the few
     * operations whose handler has no {@code ServiceResult} to wrap.
     */
    public byte[] writeBody(Object body) {
        if (body == null) {
            return NO_BODY;
        }
        if (body instanceof byte[]) {
            return (byte[]) body;
        }
        if (body instanceof String) {
            return ((String) body).getBytes(StandardCharsets.UTF_8);
        }
        try {
            return mapper.writeValueAsBytes(body);
        } catch (JsonProcessingException e) {
            throw new DlRuntimeException("failed to serialize the response body", e);
        }
    }
}
