package com.trendyol.cbcrudpoc.repository.cocuhbase;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.util.retry.RetryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static rx.schedulers.Schedulers.io;

@Repository
public abstract class AbstractCouchbaseRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCouchbaseRepository.class);
    private static final Long BULK_TIMEOUT_IN_MS = 30000L;
    private static final Long SINGLE_TIMEOUT_IN_MS = 3000L;
    private static final int RETRY_MAX_ATTEMPTS = 3;
    private static final Long RETRY_DELAY_IN_MS = 300L;

    private final AsyncBucket bucket;

    protected AbstractCouchbaseRepository(Bucket bucket) {
        this.bucket = bucket.async();
    }

    /* Non-blocking Crud Operations */
    public Observable<JsonDocument> getByIdNonBlocking(String documentId) {
        return Observable.just(documentId)
                .flatMap(bucket::get)
                .observeOn(io())
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .onErrorResumeNext(ex -> {
                    LOGGER.warn("Error when getting document by documentId: {} from replica. Exception : {}", documentId, ex);
                    return Observable.just(documentId)
                            .flatMap(id -> bucket.getFromReplica(id, ReplicaMode.ALL))
                            .observeOn(io())
                            .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                            .onErrorResumeNext(secondEx -> {
                                LOGGER.warn("Error when getting document by documentId: {} from replica. Exception : {}", documentId, ex);
                                return Observable.empty();
                            });
                })
                .firstOrDefault(null);
    }

    public Observable<List<JsonDocument>> getByIdsNonBlocking(List<String> documentIds) {
        return Observable
                .from(documentIds)
                .flatMap(bucket::get)
                .observeOn(io())
                .timeout(BULK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .onErrorResumeNext(ex -> {
                    String commaSeparatedIds = String.join(", ", documentIds);
                    LOGGER.warn("Error when getting documents by ids: {}, Exception : {}", commaSeparatedIds, ex);
                    return Observable.from(documentIds)
                            .observeOn(io())
                            .timeout(BULK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                            .flatMap(id -> bucket.getFromReplica(id, ReplicaMode.ALL))
                            .onErrorResumeNext(secondEx -> {
                                LOGGER.warn("Error when getting documents from replica by documentIds: " + commaSeparatedIds, secondEx);
                                return Observable.empty();
                            });
                })
                .toList()
                .firstOrDefault(new ArrayList<>(0));
    }

    public void insertNonBlocking(RawJsonDocument document) {
        bucket.insert(document)
                .observeOn(Schedulers.io())
                .doOnError(ex -> LOGGER.error("Error when inserting document. Id: {} - Document: {} - Exception : {}", document.id(), document.content(), ex))
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .retryWhen(RetryBuilder.allBut(DocumentAlreadyExistsException.class).max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .first();
    }

    public Observable<JsonDocument> removeNonBlocking(String id) {
        return bucket.remove(id)
                .observeOn(Schedulers.io())
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .retryWhen(RetryBuilder.any().max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .doOnError(ex -> LOGGER.error("Error when deleting document with id: {} - Exception : {}" ,id, ex))
                .firstOrDefault(null);
    }

    public void updateNonBlocking(RawJsonDocument document) {
        bucket.replace(document)
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .doOnError(ex -> LOGGER.error("Error when updating document. Id: {} - Document: {} - Exception : {}", document.id(), document.content(), ex))
                .retryWhen(RetryBuilder.allBut(DocumentDoesNotExistException.class, CASMismatchException.class).max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .observeOn(io())
                .first();
    }

    public Observable<List<JsonObject>> filterNonBlocking(Statement query) {
        return bucket.query(query)
                .flatMap(AsyncN1qlQueryResult::rows)
                .observeOn(Schedulers.io())
                .doOnError(ex -> LOGGER.error("Error when executing query on couchbase. Query: {} - Exception : {}", query, ex))
                .timeout(BULK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .map(AsyncN1qlQueryRow::value)
                .toList()
                .firstOrDefault(null);
    }

    public Observable<Boolean> isDocumentFoundNonBlocking(String id) {
        return bucket.exists(id)
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .retryWhen(RetryBuilder.any().max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .observeOn(io())
                .firstOrDefault(false);
    }

    /* Blocking Crud Operations */
    public JsonDocument getByIdBlocking(String documentId) {
        return Observable.just(documentId)
                .flatMap(bucket::get)
                .observeOn(io())
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .onErrorResumeNext(ex -> {
                    LOGGER.warn("Error when getting document by documentId: {} from replica. Exception : {}", documentId, ex);
                    return Observable.just(documentId)
                            .flatMap(id -> bucket.getFromReplica(id, ReplicaMode.ALL))
                            .observeOn(io())
                            .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                            .onErrorResumeNext(secondEx -> {
                                LOGGER.warn("Error when getting document by documentId: {} from replica. Exception : {}", documentId, ex);
                                return Observable.empty();
                            });
                })
                .toBlocking()
                .firstOrDefault(null);
    }

    public List<JsonDocument> getByIdsBlocking(List<String> documentIds) {
        return Observable
                .from(documentIds)
                .flatMap(bucket::get)
                .observeOn(io())
                .timeout(BULK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .onErrorResumeNext(ex -> {
                    String commaSeparatedIds = String.join(", ", documentIds);
                    LOGGER.warn("Error when getting documents by ids: {}, Exception : {}", commaSeparatedIds, ex);
                    return Observable.from(documentIds)
                            .observeOn(io())
                            .timeout(BULK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                            .flatMap(id -> bucket.getFromReplica(id, ReplicaMode.ALL))
                            .onErrorResumeNext(secondEx -> {
                                LOGGER.warn("Error when getting documents from replica by documentIds: " + commaSeparatedIds, secondEx);
                                return Observable.empty();
                            });
                })
                .toList()
                .toBlocking()
                .firstOrDefault(new ArrayList<>(0));
    }

    public void insertBlocking(RawJsonDocument document) {
        bucket.insert(document)
                .observeOn(Schedulers.io())
                .doOnError(ex -> LOGGER.error("Error when inserting document. Id: {} - Document: {} - Exception : {}", document.id(), document.content(), ex))
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .retryWhen(RetryBuilder.allBut(DocumentAlreadyExistsException.class).max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .toBlocking()
                .first();
    }

    public JsonDocument removeBlocking(String id) {
        return bucket.remove(id)
                .observeOn(Schedulers.io())
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .retryWhen(RetryBuilder.any().max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .doOnError(ex -> LOGGER.error("Error when deleting document with id: {} - Exception : {}" ,id, ex))
                .toBlocking()
                .firstOrDefault(null);
    }

    public void updateBlocking(RawJsonDocument document) {
        bucket.replace(document)
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .doOnError(ex -> LOGGER.error("Error when updating document. Id: {} - Document: {} - Exception : {}", document.id(), document.content(), ex))
                .retryWhen(RetryBuilder.allBut(DocumentDoesNotExistException.class, CASMismatchException.class).max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .observeOn(io())
                .toBlocking()
                .first();
    }

    public List<JsonObject> filterBlocking(Statement query) {
        return bucket.query(query)
                .flatMap(AsyncN1qlQueryResult::rows)
                .observeOn(Schedulers.io())
                .doOnError(ex -> LOGGER.error("Error when executing query on couchbase. Query: {} - Exception : {}", query, ex))
                .timeout(BULK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .map(AsyncN1qlQueryRow::value)
                .toList()
                .toBlocking()
                .firstOrDefault(null);
    }

    public Boolean isDocumentFoundBlocking(String id) {
        return bucket.exists(id)
                .timeout(SINGLE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)
                .retryWhen(RetryBuilder.any().max(RETRY_MAX_ATTEMPTS).delay(Delay.fixed(RETRY_DELAY_IN_MS, TimeUnit.MILLISECONDS)).build())
                .observeOn(io())
                .toBlocking()
                .firstOrDefault(false);
    }
}