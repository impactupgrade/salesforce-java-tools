package com.impactupgrade.integration.sfdc;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.MergeRequest;
import com.sforce.soap.partner.MergeResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SFDCPartnerAPIClient {

  private static final Logger log = LogManager.getLogger(SFDCPartnerAPIClient.class.getName());

  private static final class AuthContext {
    private String username;
    private String password;
    private String url;

    // Implement equals/hashcode so the Guava cache can use this as a key!

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AuthContext that = (AuthContext) o;
      return username.equals(that.username) &&
          password.equals(that.password) &&
          url.equals(that.url);
    }

    @Override
    public int hashCode() {
      return Objects.hash(username, password, url);
    }
  }

  private final AuthContext authContext;
  private final Supplier<PartnerConnection> partnerConnection;
  private final int defaultBatchSize;
  // 200 is the max allowed by the API
  private static final int MAX_BATCH_SIZE = 200;

  // The number of min we can cache and continue to use an open session to the SFDC API. In SFDC Setup, the
  // Session Settings allow you to configure a timeout value as low as 15 min (we default to that, just in case).
  // But that's configurable up to 24 hours. Pass in a sessionTtlMin value that matches your settings
  // to minimize the number of login calls!
  private static final String _sessionTtlMin = System.getenv("SFDC_SESSION_TIMEOUT_MIN");
  private static final int sessionTtlMin = Strings.isNullOrEmpty(_sessionTtlMin) ? 15 : Integer.parseInt(_sessionTtlMin);

  // The creation of PartnerConnection is expensive, as it provisions itself and calls SF to log in. Instead of
  // attempting to optimize using ThreadLocal (memory leaks + not super helpful for large, concurrent use cases like
  // getting slammed by a particular webhook), we opt to use a simple Guava cache with a TTL defined by
  // sessionTtlMin. Key it off the AuthContext. Lazily initialize it so the first caller can configure the TTL.
  private static final LoadingCache<AuthContext, PartnerConnection> partnerConnectionCache = CacheBuilder.newBuilder()
      .expireAfterAccess(sessionTtlMin, TimeUnit.MINUTES)
      .build(new CacheLoader<>() {
        @Override
        public PartnerConnection load(AuthContext authContext) {
          log.info("loading new SFDC connection for {}", authContext.username);

          ConnectorConfig connectorConfig = new ConnectorConfig();
          connectorConfig.setUsername(authContext.username);
          connectorConfig.setPassword(authContext.password);
          connectorConfig.setAuthEndpoint(authContext.url);

//          connectorConfig.setTraceMessage(true);
//          connectorConfig.setPrettyPrintXml(true);
          try {
            PartnerConnection partnerConnection = Connector.newConnection(connectorConfig);

            // Fuzzy duplicating detection rules are super helpful when using the web dashboard, but can wreck havoc
            // on data integrations through the API. Globally disable duplicate checks on inserts/updates.
            partnerConnection.setDuplicateRuleHeader(true, false, false);

            return partnerConnection;
          } catch (ConnectionException e) {
            throw new RuntimeException(e);
          }
        }
      });

  public SFDCPartnerAPIClient(String username, String password, String url, int defaultBatchSize) {
    authContext = new AuthContext();
    authContext.username = username;
    authContext.password = password;
    authContext.url = url;

    partnerConnection = () -> {
      try {
        return partnerConnectionCache.get(authContext);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    };

    this.defaultBatchSize = defaultBatchSize;
  }

  /**
   * Note that we're not actually calling this method (partnerConnection, further down, handles its own auth).
   * However, other SFDC integration APIs (Bulk, Metadata, etc.) need to login through SOAP in order to get a sessionId
   * and the server endpoints. We supply this endpoint as a helper.
   */
  public LoginResult login() throws ConnectionException {
    // Callers of this method are always manual tasks from the UI, so it's ok to not retry the connection.
    LoginResult loginResult = partnerConnection.get().login(authContext.username, authContext.password);
    log.info("partner endpoint: " + loginResult.getServerUrl());
    log.info("metadata endpoint: " + loginResult.getMetadataServerUrl());
    return loginResult;
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Map between SFDC Enterprise API (strongly typed model for a custom schema) and SFDC Partner API (generic SObject)
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public <T> List<T> toEnterprise(Class<T> eClass, List<SObject> sObjects) {
    return sObjects.stream().map(o -> toEnterprise(eClass, o)).collect(Collectors.toList());
  }
  public <T> T[] toEnterprise(Class<T> eClass, SObject[] sObjects) {
    T[] results = (T[]) Array.newInstance(eClass, sObjects.length);
    for (int i = 0; i < sObjects.length; i++) {
      results[i] = toEnterprise(eClass, sObjects[i]);
    }
    return results;
  }
  public <T> Optional<T> toEnterprise(Class<T> eClass, Optional<SObject> sObject) {
    return sObject.map(o -> toEnterprise(eClass, o));
  }

  public <T> T toEnterprise(Class<T> eClass, SObject sObject) {
    try {
      T object = eClass.getDeclaredConstructor().newInstance();

      // a few special cases
      Method id = object.getClass().getMethod("setId", String.class);
      id.invoke(object, sObject.getId());
      Method fieldsToNull = object.getClass().getMethod("setFieldsToNull", String[].class);
      fieldsToNull.invoke(object, new Object[]{sObject.getFieldsToNull()});

      for (Field field : object.getClass().getDeclaredFields()) {
        String name = field.getName();

        if (
            "typeInfoCache".equalsIgnoreCase(name)
        ) {
          continue;
        } else {
          Object value = sObject.getSObjectField(name);

          if (value instanceof SObject) {
            // if nested object, recursively build it
            value = toEnterprise(field.getType(), (SObject) value);
          } else if (value != null && Calendar.class.equals(field.getType())) {
            // map String to Calendar, date only (no time)
            Calendar c = Calendar.getInstance();
            c.setTime(new SimpleDateFormat("yyyy-MM-dd").parse((String) value));
            value = c;
          }

          // handle defaults for primitive wrappers if the value was null
          Object defaultValueForNull = null;
          if (Boolean.class.equals(field.getType())) {
            if (value == null) {
              defaultValueForNull = false;
            } else {
              value = Boolean.parseBoolean((String) value);
            }
          } else if (value != null && Byte.class.equals(field.getType())) {
            value = Byte.valueOf((String) value);
          } else if (value != null && Character.class.equals(field.getType())) {
            value = ((String) value).charAt(0);
          } else if (Double.class.equals(field.getType())) {
            if (value == null) {
              defaultValueForNull = 0d;
            } else {
              value = Double.parseDouble((String) value);
            }
          } else if (Float.class.equals(field.getType())) {
            if (value == null) {
              defaultValueForNull = 0f;
            } else {
              value = Float.parseFloat((String) value);
            }
          } else if (Integer.class.equals(field.getType())) {
            if (value == null) {
              defaultValueForNull = 0;
            } else {
              value = Integer.parseInt((String) value);
            }
          } else if (Long.class.equals(field.getType())) {
            if (value == null) {
              defaultValueForNull = 0L;
            } else {
              value = Long.parseLong((String) value);
            }
          } else if (Short.class.equals(field.getType())) {
            if (value == null) {
              defaultValueForNull = 0;
            } else {
              value = Short.parseShort((String) value);
            }
          }

          try {
            if (defaultValueForNull != null) {
              // IMPORTANT: Enterprise API objects set an is_set flag in the property setters, which is then used to
              // determine what fields to include when the object is marshalled back into XML to be sent for an update.
              // We want defaults to be read only, not later persisted! Therefore, set the is_set flag, but only if we
              // do *not* have a default value (for Boolean and numerics).
              field.setAccessible(true);
              field.set(object, defaultValueForNull);
            } else if (value != null) {
              field.setAccessible(true);
              field.set(object, value);

              Field isSetField = object.getClass().getDeclaredField(name + "__is_set");
              isSetField.setAccessible(true);
              isSetField.set(object, true);
            }
          } catch (Exception e) {
            log.error("failed to set value {} on {}.{}", value, eClass.getSimpleName(), name);
            // re-throw so the next layer catches it
            throw new RuntimeException(e);
          }
        }
      }

      return object;
    } catch (Exception e) {
      log.error(e);
      // re-throw -- consider this catastrophic and halt the thread
      throw new RuntimeException(e);
    }
  }

  public List<SObject> toPartner(List<Object> objects) {
    return objects.stream().map(this::toPartner).collect(Collectors.toList());
  }
  public SObject[] toPartner(Object[] objects) {
    return Arrays.stream(objects).map(this::toPartner).toArray(SObject[]::new);
  }
  public Optional<SObject> toPartner(Optional<Object> object) {
    return object.map(this::toPartner);
  }

  public SObject toPartner(Object object) {
    if (object instanceof SObject) {
      return (SObject) object;
    }

    SObject sObject = new SObject(object.getClass().getSimpleName());

    try {
      // a few special cases
      Method id = object.getClass().getMethod("getId");
      sObject.setId((String) id.invoke(object));
      Method fieldsToNull = object.getClass().getMethod("getFieldsToNull");
      sObject.setFieldsToNull((String[]) fieldsToNull.invoke(object));

      for (Field field : object.getClass().getDeclaredFields()) {
        String name = field.getName();

        if (
            "typeInfoCache".equalsIgnoreCase(name)
                || field.getName().endsWith("__is_set")
        ) {
          continue;
        } else {
          field.setAccessible(true);
          Object value = field.get(object);

          if (value == null) {
            continue;
          }

          Field fieldIsSet = object.getClass().getDeclaredField(name + "__is_set");
          fieldIsSet.setAccessible(true);
          boolean isSet = (boolean) fieldIsSet.get(object);
          // See note on defaultValueForNull. Only write the field if the setter was used, *not* default values.
          if (isSet) {
            if (value.getClass().getName().contains("com.sforce.soap.enterprise.sobject")) {
              // nested enterprise object -- convert it
              sObject.setSObjectField(name, toPartner(value));
            } else {
              sObject.setSObjectField(name, value);
            }
          }
        }
      }

      return sObject;
    } catch (Exception e) {
      log.error(e);
      // re-throw -- consider this catastrophic and halt the thread
      throw new RuntimeException(e);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Wrappers for queries, inserts, updates, and deletes.
  //
  // Some events affecting the same account/contact/recurring-donation can come in nearly simultaneously,
  // especially if this integration is ever paired with payment gateway webhooks, etc.
  // Ex: If Stripe is closing a subscription, we'll get the charge.failed and customer.subscription.deleted at
  // the same time. You can wind up with an UNABLE_TO_LOCK_ROW error, since both events are acting upon the same parents.
  // SFDC also randomly likes to time out and throw ConnectionExceptions, so gracefully handle that too.
  // These methods support retries on 10 sec intervals, up to a minute.
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public List<SObject> queryList(String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString).getRecords())
        .collect(Collectors.toList());
  }

  public <T> List<T> queryList(Class<T> eClass, String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString).getRecords())
        .map(sObject -> toEnterprise(eClass, sObject))
        .collect(Collectors.toList());
  }

  public List<SObject> queryListAutoPaged(String queryString)
      throws ConnectionException, InterruptedException {
    return _queryListAutoPaged(queryString, null);
  }

  private List<SObject> _queryListAutoPaged(String queryString, String previousQueryLocator)
      throws ConnectionException, InterruptedException {
    QueryResult queryResult;
    if (previousQueryLocator == null) {
      queryResult = _query(0, queryString);
    } else {
      queryResult = _queryMore(0, previousQueryLocator);
    }

    // Silly, but needs to be mutable.
    List<SObject> records = new ArrayList<>(Arrays.asList(queryResult.getRecords()));

    if (!queryResult.isDone()) {
      records.addAll(_queryListAutoPaged(queryString, queryResult.getQueryLocator()));
    }

    return records;
  }

  public Optional<SObject> querySingle(String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString).getRecords())
        .findFirst();
  }

  public <T> Optional<T> querySingle(Class<T> eClass, String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString).getRecords())
        .map(sObject -> toEnterprise(eClass, sObject))
        .findFirst();
  }

  public long queryCount(String queryString) throws ConnectionException, InterruptedException {
    return _query(0, queryString).getSize();
  }

  public QueryResult query(String queryString) throws ConnectionException, InterruptedException {
    return _query(0, queryString);
  }

  public SaveResult[] insert(List<Object> objects) throws InterruptedException {
    return _insert(0, toPartner(objects).toArray(new SObject[0]));
  }
  public SaveResult[] insert(Object[] objects) throws InterruptedException {
    return _insert(0, toPartner(objects));
  }
  public SaveResult insert(Object object) throws InterruptedException {
    return _insert(0, new SObject[]{toPartner(object)})[0];
  }

  public SaveResult[] update(List<Object> objects) throws InterruptedException {
    return _update(0, toPartner(objects).toArray(new SObject[0]));
  }
  public SaveResult[] update(Object[] objects) throws InterruptedException {
    return _update(0, toPartner(objects));
  }
  public SaveResult update(Object object) throws InterruptedException {
    return _update(0, new SObject[]{toPartner(object)})[0];
  }

  public DeleteResult[] delete(List<Object> objects) throws InterruptedException {
    return _delete(0, toPartner(objects).toArray(new SObject[0]));
  }
  public DeleteResult[] delete(Object[] objects) throws InterruptedException {
    return _delete(0, toPartner(objects));
  }
  public DeleteResult delete(Object object) throws InterruptedException {
    return _delete(0, new SObject[]{toPartner(object)})[0];
  }

  public MergeResult[] merge(List<MergeRequest> mergeRequests) throws InterruptedException {
    return _merge(0, mergeRequests.toArray(new MergeRequest[0]));
  }
  public MergeResult[] merge(MergeRequest[] mergeRequests) throws InterruptedException {
    return _merge(0, mergeRequests);
  }
  public MergeResult merge(MergeRequest mergeRequest) throws InterruptedException {
    return _merge(0, new MergeRequest[]{mergeRequest})[0];
  }

  private final ThreadLocal<List<SObject>> batchInserts = ThreadLocal.withInitial(ArrayList::new);
  private final ThreadLocal<List<SaveResult>> batchInsertResults = ThreadLocal.withInitial(ArrayList::new);
  private final ThreadLocal<List<SObject>> batchUpdates = ThreadLocal.withInitial(ArrayList::new);
  private final ThreadLocal<List<SaveResult>> batchUpdateResults = ThreadLocal.withInitial(ArrayList::new);
  private final ThreadLocal<List<SObject>> batchDeletes = ThreadLocal.withInitial(ArrayList::new);

  public void batchInsert(List<Object> objects) throws InterruptedException {
    for (Object object : objects) {
      batchInsert(defaultBatchSize, object);
    }
  }
  public void batchInsert(Object[] objects) throws InterruptedException {
    for (Object object : objects) {
      batchInsert(defaultBatchSize, object);
    }
  }
  public void batchInsert(int batchSize, List<Object> objects) throws InterruptedException {
    for (Object object : objects) {
      batchInsert(batchSize, object);
    }
  }
  public void batchInsert(int batchSize, Object[] objects) throws InterruptedException {
    for (Object object : objects) {
      batchInsert(batchSize, object);
    }
  }
  public void batchInsert(Object object) throws InterruptedException {
    batchInsert(defaultBatchSize, object);
  }
  public void batchInsert(int batchSize, Object object) throws InterruptedException {
    batchSize = Math.min(batchSize, MAX_BATCH_SIZE);

    batchInserts.get().add(toPartner(object));

    if (batchInserts.get().size() >= batchSize) {
      SaveResult[] results = insert(prepareBatchActions(batchInserts.get()));
      batchInsertResults.get().addAll(Arrays.stream(results).toList());
      batchInserts.get().clear();
    }
  }

  public void batchUpdate(List<Object> objects) throws InterruptedException {
    for (Object object : objects) {
      batchUpdate(defaultBatchSize, object);
    }
  }
  public void batchUpdate(Object[] objects) throws InterruptedException {
    for (Object object : objects) {
      batchUpdate(defaultBatchSize, object);
    }
  }
  public void batchUpdate(int batchSize, List<Object> objects) throws InterruptedException {
    for (Object object : objects) {
      batchUpdate(batchSize, object);
    }
  }
  public void batchUpdate(int batchSize, Object[] objects) throws InterruptedException {
    for (Object object : objects) {
      batchUpdate(batchSize, object);
    }
  }
  public void batchUpdate(Object object) throws InterruptedException {
    batchUpdate(defaultBatchSize, object);
  }
  public void batchUpdate(int batchSize, Object object) throws InterruptedException {
    batchSize = Math.min(batchSize, MAX_BATCH_SIZE);

    batchUpdates.get().add(toPartner(object));

    if (batchUpdates.get().size() >= batchSize) {
      SaveResult[] results = update(prepareBatchActions(batchUpdates.get()));
      batchUpdateResults.get().addAll(Arrays.stream(results).toList());
      batchUpdates.get().clear();
    }
  }

  public void batchDelete(List<Object> objects) throws InterruptedException {
    for (Object object : objects) {
      batchDelete(defaultBatchSize, object);
    }
  }
  public void batchDelete(Object[] objects) throws InterruptedException {
    for (Object object : objects) {
      batchDelete(defaultBatchSize, object);
    }
  }
  public void batchDelete(int batchSize, List<Object> objects) throws InterruptedException {
    for (Object object : objects) {
      batchDelete(batchSize, object);
    }
  }
  public void batchDelete(int batchSize, Object[] objects) throws InterruptedException {
    for (Object object : objects) {
      batchDelete(batchSize, object);
    }
  }
  public void batchDelete(Object object) throws InterruptedException {
    batchDelete(defaultBatchSize, object);
  }
  public void batchDelete(int batchSize, Object object) throws InterruptedException {
    batchSize = Math.min(batchSize, MAX_BATCH_SIZE);

    batchDeletes.get().add(toPartner(object));

    if (batchDeletes.get().size() >= batchSize) {
      delete(prepareBatchActions(batchDeletes.get()));
      batchDeletes.get().clear();
    }
  }

  private SObject[] prepareBatchActions(List<SObject> batchActions) {
    // The sorting is an *important* optimization.
    // Say you have a list of updates with Contact, Account, Contact, Account, ... Salesforce treats each change of
    // SObject type in a sequence as a "chunk". Although the API allows up to 200 record changes per call, it only
    // allows 10 "chunks". So if we change that list to Account, Account, ..., Contact, Contact, ... it only counts as
    // 2 chunks since the type only changes once in the sequence.
    return batchActions.stream()
        .sorted(Comparator.comparing(SObject::getType)).toArray(SObject[]::new);
  }

  /**
   * Flush out and act upon any remaining batch operations. This is important to call after your looping logic is
   * finished, since the batch queue will almost always contain additional operations that haven't executed yet
   * (depending on your batchSize). Also important to call in a try/final to ensure memory is cleaned up during an
   * error condition!
   *
   * @throws InterruptedException
   */
  public record BatchResults(List<SaveResult> batchInsertResults, List<SaveResult> batchUpdateResults){}
  public BatchResults batchFlush() throws InterruptedException {
    if (!batchInserts.get().isEmpty()) {
      SaveResult[] results = insert(prepareBatchActions(batchInserts.get()));
      batchInsertResults.get().addAll(Arrays.stream(results).toList());
      batchInserts.get().clear();
    }
    if (!batchUpdates.get().isEmpty()) {
      SaveResult[] results = update(prepareBatchActions(batchUpdates.get()));
      batchUpdateResults.get().addAll(Arrays.stream(results).toList());
      batchUpdates.get().clear();
    }
    if (!batchDeletes.get().isEmpty()) {
      delete(prepareBatchActions(batchDeletes.get()));
      batchDeletes.get().clear();
    }

    // clone the results, clear out the original lists, and return in one payload
    List<SaveResult> _batchInsertResults = new ArrayList<>();
    Iterator<SaveResult> itr = batchInsertResults.get().iterator();
    while (itr.hasNext()) {
      _batchInsertResults.add(itr.next());
      itr.remove();
    }
    List<SaveResult> _batchUpdateResults = new ArrayList<>();
    itr = batchUpdateResults.get().iterator();
    while (itr.hasNext()) {
      _batchUpdateResults.add(itr.next());
      itr.remove();
    }
    return new BatchResults(_batchInsertResults, _batchUpdateResults);
  }

  // Helper method for adding a file attachment to a record, using the more modern approach (Content API, as opposed
  // to the old Attachment API).
  public void uploadAttachment(String filename, byte[] content, String targetId) throws InterruptedException, ConnectionException {
    SObject contentVersion = new SObject("ContentVersion");
    contentVersion.setField("PathOnClient", filename);
    contentVersion.setField("VersionData", content);
    SaveResult saveResult = insert(contentVersion);

    if (saveResult.isSuccess()) {
      // Implicitly created a ContentDocument -- need to retrieve its ID for use with ContentDocumentLink.
      contentVersion = querySingle("SELECT ContentDocumentId FROM ContentVersion WHERE Id='" + saveResult.getId() + "'").get();

      SObject contentDocumentLink = new SObject("ContentDocumentLink");
      contentDocumentLink.setField("ContentDocumentId", contentVersion.getField("ContentDocumentId"));
      contentDocumentLink.setField("LinkedEntityId", targetId);
      insert(contentDocumentLink);
    }
  }

  public Map<String, Object> toMap(SObject sObject) {
    return toMap(sObject, "");
  }

  private Map<String, Object> toMap(XmlObject xmlObject, String prefix) {
    Map<String, Object> map = new HashMap<>();

    Iterator<XmlObject> children = xmlObject.getChildren();
    while (children.hasNext()) {
      XmlObject child = children.next();
      if (child.hasChildren()) {
        map.putAll(toMap(child, prefix + child.getName().getLocalPart() + "."));
      } else {
        map.put(prefix + child.getName().getLocalPart(), child.getValue());
      }
    }

    return map;
  }

  // Some use cases (ex: object mapper frameworks) can better handle conversions from Strings, versus the
  // ambiguity of Object.
  public Map<String, String> toMapOfStrings(SObject sObject) {
    return toMap(sObject, "").entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
      Object value = e.getValue();
      return value == null ? "" : value + "";
    }));
  }

  private QueryResult _query(int count, String queryString) throws ConnectionException, InterruptedException {
    try {
      return partnerConnection.get().query(queryString);
    } catch (ApiFault e) {
      log.error("query failed due to {}: {}", e.getExceptionCode(), e.getExceptionMessage(), e);
      throw e;
    } catch (ConnectionException e) {
      log.warn("query attempt {} failed due to connection issues; retrying in 5s", count, e);
      Thread.sleep(5000);

      if (count == 5) {
        log.error("unable to complete query by attempt {}", count);
        // rethrow exception, since the whole flow simply needs to halt at this point
        throw e;
      }

      return _query(count + 1, queryString);
    }
  }

  private QueryResult _queryMore(int count, String previousQueryLocator) throws ConnectionException, InterruptedException {
    try {
      return partnerConnection.get().queryMore(previousQueryLocator);
    } catch (ApiFault e) {
      log.error("query failed due to {}: {}", e.getExceptionCode(), e.getExceptionMessage(), e);
      throw e;
    } catch (ConnectionException e) {
      log.warn("query attempt {} failed due to connection issues; retrying in 5s", count, e);
      Thread.sleep(5000);

      if (count == 5) {
        log.error("unable to complete query by attempt {}", count);
        // rethrow exception, since the whole flow simply needs to halt at this point
        throw e;
      }

      return _queryMore(count + 1, previousQueryLocator);
    }
  }

  private SaveResult[] _insert(int count, SObject[] sObjects) throws InterruptedException {
    String clazz = sObjects[0].getType();

    if (count == 6) {
      log.error("unable to complete insert {} by attempt {}", clazz, count);
      SaveResult saveResult = new SaveResult();
      saveResult.setSuccess(false);
      return new SaveResult[]{saveResult};
    }

    log.info("inserting {}", clazz);

    try {
      SaveResult[] saveResults = partnerConnection.get().create(sObjects);

      for (int i = 0; i < saveResults.length; i++) {
        SaveResult saveResult = saveResults[i];

        log.info(saveResult);

        // if any insert failed due to a row lock, retry that single object on its own
        // a few different types of lock-related error codes, so don't use the enum itself
        if (!saveResult.isSuccess() && Arrays.stream(saveResult.getErrors())
            .anyMatch(e -> e.getStatusCode() != null
                && (e.getStatusCode().toString().contains("LOCK") || e.getMessage().contains("LOCK")))) {
          log.info("insert attempt {} failed due to locks; retrying {} {} in 5s", count, clazz, i);
          Thread.sleep(5000);
          SObject sObject = sObjects[i];
          SaveResult retryResult = _insert(count + 1, new SObject[]{sObject})[0];
          saveResults[i] = retryResult;
        }
      }

      return saveResults;
    } catch (ApiFault e) {
      log.error("query failed due to {}: {}", e.getExceptionCode(), e.getExceptionMessage(), e);

      SaveResult saveResult = new SaveResult();
      saveResult.setSuccess(false);
      return new SaveResult[]{saveResult};
    } catch (ConnectionException e) {
      log.warn("insert attempt {} failed due to connection issues; retrying {} in 5s", count, clazz, e);
      Thread.sleep(5000);
      return _insert(count + 1, sObjects);
    }
  }

  private SaveResult[] _update(int count, SObject[] sObjects) throws InterruptedException {
    String clazz = sObjects[0].getType();
    String ids = Arrays.stream(sObjects).map(SObject::getId).collect(Collectors.joining(","));

    if (count == 6) {
      log.error("unable to complete update {} {} by attempt {}", clazz, ids, count);
      SaveResult saveResult = new SaveResult();
      saveResult.setSuccess(false);
      return new SaveResult[]{saveResult};
    }

    log.info("updating {} {}", clazz, ids);

    try {
      SaveResult[] saveResults = partnerConnection.get().update(sObjects);

      for (int i = 0; i < saveResults.length; i++) {
        SaveResult saveResult = saveResults[i];

        log.info(saveResult);

        // if any update failed due to a row lock, retry that single object on its own
        // a few different types of lock-related error codes, so don't use the enum itself
        if (!saveResult.isSuccess() && Arrays.stream(saveResult.getErrors())
            .anyMatch(e -> e.getStatusCode() != null
                && (e.getStatusCode().toString().contains("LOCK") || e.getMessage().contains("LOCK")))) {
          log.info("update attempt {} failed due to locks; retrying {} {} in 5s", count, clazz, saveResult.getId());
          Thread.sleep(5000);
          SObject sObject = sObjects[i];
          SaveResult retryResult = _update(count + 1, new SObject[]{sObject})[0];
          saveResults[i] = retryResult;
        }
      }

      return saveResults;
    } catch (ApiFault e) {
      log.error("query failed due to {}: {}", e.getExceptionCode(), e.getExceptionMessage(), e);

      SaveResult saveResult = new SaveResult();
      saveResult.setSuccess(false);
      return new SaveResult[]{saveResult};
    } catch (ConnectionException e) {
      log.warn("update attempt {} failed due to connection issues; retrying {} {} in 5s", count, clazz, ids, e);
      Thread.sleep(5000);
      return _update(count + 1, sObjects);
    }
  }

  private DeleteResult[] _delete(int count, SObject[] sObjects) throws InterruptedException {
    String clazz = sObjects[0].getType();
    Map<String, SObject> byId = Arrays.stream(sObjects).collect(Collectors.toMap(SObject::getId, Function.identity(), (so1, so2) -> so1));
    String ids = byId.values().stream().map(SObject::getId).collect(Collectors.joining(","));

    if (count == 6) {
      log.error("unable to complete delete {} {} by attempt {}", clazz, ids, count);
      DeleteResult deleteResult = new DeleteResult();
      deleteResult.setSuccess(false);
      return new DeleteResult[]{deleteResult};
    }

    log.info("deleting {} {}", clazz, ids);

    try {
      DeleteResult[] deleteResults = partnerConnection.get().delete(byId.keySet().toArray(new String[0]));

      for (int i = 0; i < deleteResults.length; i++) {
        DeleteResult deleteResult = deleteResults[i];

        log.info(deleteResult);

        // if any delete failed due to a row lock, retry that single object on its own
        // a few different types of lock-related error codes, so don't use the enum itself
        if (!deleteResult.isSuccess() && Arrays.stream(deleteResult.getErrors())
            .anyMatch(e -> e.getStatusCode() != null
                && (e.getStatusCode().toString().contains("LOCK") || e.getMessage().contains("LOCK")))) {
          log.info("delete attempt {} failed due to locks; retrying {} {} in 5s", count, clazz, deleteResult.getId());
          Thread.sleep(5000);
          SObject sObject = sObjects[i];
          DeleteResult retryResult = _delete(count + 1, new SObject[]{sObject})[0];
          deleteResults[i] = retryResult;
        }
      }

      return deleteResults;
    } catch (ApiFault e) {
      log.error("query failed due to {}: {}", e.getExceptionCode(), e.getExceptionMessage(), e);

      DeleteResult deleteResult = new DeleteResult();
      deleteResult.setSuccess(false);
      return new DeleteResult[]{deleteResult};
    } catch (ConnectionException e) {
      log.warn("delete attempt {} failed due to connection issues; retrying {} {} in 5s", count, clazz, ids, e);
      Thread.sleep(5000);
      return _delete(count + 1, sObjects);
    }
  }

  private MergeResult[] _merge(int count, MergeRequest[] mergeRequests) throws InterruptedException {
    String clazz = mergeRequests[0].getMasterRecord().getType();
    Map<String, SObject> byId = Arrays.stream(mergeRequests).map(MergeRequest::getMasterRecord).collect(Collectors.toMap(SObject::getId, Function.identity(), (so1, so2) -> so1));
    String ids = byId.values().stream().map(SObject::getId).collect(Collectors.joining(","));

    if (count == 6) {
      log.error("unable to complete merge {} {} by attempt {}", clazz, ids, count);
      MergeResult mergeResult = new MergeResult();
      mergeResult.setSuccess(false);
      return new MergeResult[]{mergeResult};
    }

    log.info("merging {} {}", clazz, ids);

    try {
      MergeResult[] mergeResults = partnerConnection.get().merge(mergeRequests);

      for (int i = 0; i < mergeResults.length; i++) {
        MergeResult mergeResult = mergeResults[i];

        log.info(mergeResult);

        // if any merge failed due to a row lock, retry that single object on its own
        // a few different types of lock-related error codes, so don't use the enum itself
        if (!mergeResult.isSuccess() && Arrays.stream(mergeResult.getErrors())
            .anyMatch(e -> e.getStatusCode() != null
                && (e.getStatusCode().toString().contains("LOCK") || e.getMessage().contains("LOCK")))) {
          log.info("merge attempt {} failed due to locks; retrying {} {} in 5s", count, clazz, mergeResult.getId());
          Thread.sleep(5000);
          MergeRequest mergeRequest = mergeRequests[i];
          MergeResult retryResult = _merge(count + 1, new MergeRequest[]{mergeRequest})[0];
          mergeResults[i] = retryResult;
        }
      }

      return mergeResults;
    } catch (ApiFault e) {
      log.error("query failed due to {}: {}", e.getExceptionCode(), e.getExceptionMessage(), e);

      MergeResult mergeResult = new MergeResult();
      mergeResult.setSuccess(false);
      return new MergeResult[]{mergeResult};
    } catch (ConnectionException e) {
      log.warn("merge attempt {} failed due to connection issues; retrying {} {} in 5s", count, clazz, ids, e);
      Thread.sleep(5000);
      return _merge(count + 1, mergeRequests);
    }
  }
}
