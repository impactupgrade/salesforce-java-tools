package com.impactupgrade.integration.sfdc;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
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

  // most Calendar fields are simple dates, no time
  private static final DateFormat SDF = new SimpleDateFormat("yyyy-MM-dd");

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
            return Connector.newConnection(connectorConfig);
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

  protected <T> List<T> toEnterprise(Class<T> eClass, List<SObject> sObjects) {
    return sObjects.stream().map(o -> toEnterprise(eClass, o)).collect(Collectors.toList());
  }

  protected <T> Optional<T> toEnterprise(Class<T> eClass, Optional<SObject> sObject) {
    return sObject.map(o -> toEnterprise(eClass, o));
  }

  protected <T> T toEnterprise(Class<T> eClass, SObject sObject) {
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
            c.setTime(SDF.parse((String) value));
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

  protected List<SObject> toPartner(List<Object> objects) {
    return objects.stream().map(this::toPartner).collect(Collectors.toList());
  }

  protected SObject[] toPartner(Object... objects) {
    return Arrays.stream(objects).map(this::toPartner).toArray(SObject[]::new);
  }

  protected Optional<SObject> toPartner(Optional<Object> object) {
    return object.map(this::toPartner);
  }

  protected SObject toPartner(Object object) {
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
          if (isSet) {
            // See note on defaultValueForNull. Only write the field if the setter was used, *not* default values.
            sObject.setSObjectField(name, value);
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

  protected List<SObject> queryList(String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString, null))
        .collect(Collectors.toList());
  }

  protected <T> List<T> queryList(Class<T> eClass, String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString, null))
        .map(sObject -> toEnterprise(eClass, sObject))
        .collect(Collectors.toList());
  }

  protected Optional<SObject> querySingle(String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString, null))
        .findFirst();
  }

  protected <T> Optional<T> querySingle(Class<T> eClass, String queryString) throws ConnectionException, InterruptedException {
    return Stream.of(_query(0, queryString, null))
        .map(sObject -> toEnterprise(eClass, sObject))
        .findFirst();
  }

  // Note on inserts: we're only allowing one at a time and disallowing batching. The API does not allow multiple
  // records of the same type to be inserted without explicit IDs. We're always inserting with null IDs, allowing
  // SF to auto-generate them. :(

  public SaveResult insert(Object object) throws InterruptedException {
    log.info("insert {}", object.getClass().getSimpleName());
    return _insert(0, toPartner(object));
  }

  public SaveResult[] update(Object... objects) throws InterruptedException {
    Arrays.stream(objects).forEach(object -> log.info("update", object.getClass().getSimpleName()));
    return _update(0, toPartner(objects));
  }

  public DeleteResult[] delete(Object... objects) throws InterruptedException {
    Arrays.stream(objects).forEach(object -> log.info("delete", object.getClass().getSimpleName()));
    return _delete(0, toPartner(objects));
  }

  private final ThreadLocal<List<SObject>> batchUpdates = ThreadLocal.withInitial(ArrayList::new);
  private final ThreadLocal<List<SObject>> batchDeletes = ThreadLocal.withInitial(ArrayList::new);

  public void batchUpdate(Object... objects) throws InterruptedException {
    batchUpdate(defaultBatchSize, toPartner(objects));
  }
  public void batchUpdate(int batchSize, Object... objects) throws InterruptedException {
    batchSize = Math.min(batchSize, MAX_BATCH_SIZE);

    batchUpdates.get().addAll(Arrays.asList(toPartner(objects)));

    if (batchUpdates.get().size() >= batchSize) {
      update(prepareBatchActions(batchUpdates.get()));
      batchUpdates.get().clear();
    }
  }

  public void batchDelete(Object... objects) throws InterruptedException {
    batchDelete(defaultBatchSize, toPartner(objects));
  }
  public void batchDelete(int batchSize, Object... objects) throws InterruptedException {
    batchSize = Math.min(batchSize, MAX_BATCH_SIZE);

    batchDeletes.get().addAll(Arrays.asList(toPartner(objects)));

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
  public void batchFlush() throws InterruptedException {
    if (!batchUpdates.get().isEmpty()) {
      update(prepareBatchActions(batchUpdates.get()));
      batchUpdates.get().clear();
    }
    if (!batchDeletes.get().isEmpty()) {
      delete(prepareBatchActions(batchDeletes.get()));
      batchDeletes.get().clear();
    }
  }

  private SObject[] _query(int count, String queryString, ConnectionException previousException) throws ConnectionException, InterruptedException {
    if (count == 6) {
      log.error("unable to complete query by attempt {}", count);
      // rethrow the last exception, since the whole flow simply needs to halt at this point
      throw previousException;
    }

    try {
      return partnerConnection.get().query(queryString).getRecords();
    } catch (ConnectionException e) {
      log.warn("query attempt {} failed due to connection issues; retrying in 10s", count, e);
      Thread.sleep(10000);
      return _query(count + 1, queryString, e);
    }
  }

  private SaveResult _insert(int count, SObject sObject) throws InterruptedException {
    String clazz = sObject.getClass().getSimpleName();

    if (count == 6) {
      log.error("unable to complete insert {} by attempt {}", clazz, count);
      SaveResult saveResult = new SaveResult();
      saveResult.setSuccess(false);
      return saveResult;
    }

    log.info("inserting {}", clazz);

    try {
      SaveResult saveResult = partnerConnection.get().create(new SObject[]{sObject})[0];
      log.info(saveResult);

      // if any insert failed due to a row lock, retry that single object on its own
      // a few different types of lock-related error codes, so don't use the enum itself
      if (!saveResult.isSuccess() && Arrays.stream(saveResult.getErrors())
          .anyMatch(e -> e.getStatusCode() != null
              && (e.getStatusCode().toString().contains("LOCK") || e.getMessage().contains("LOCK")))) {
        log.info("insert attempt {} failed due to locks; retrying {} {} in 10s", count, clazz, saveResult.getId());
        Thread.sleep(10000);
        return _insert(count + 1, sObject);
      }

      return saveResult;
    } catch (ConnectionException e) {
      log.warn("insert attempt {} failed due to connection issues; retrying {} in 10s", count, clazz, e);
      Thread.sleep(10000);
      return _insert(count + 1, sObject);
    }
  }

  private SaveResult[] _update(int count, SObject... sObjects) throws InterruptedException {
    String clazz = sObjects[0].getClass().getSimpleName();
    Map<String, SObject> byId = Arrays.stream(sObjects).collect(Collectors.toMap(SObject::getId, Function.identity()));
    String ids = byId.values().stream().map(SObject::getId).collect(Collectors.joining(","));

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
          log.info("update attempt {} failed due to locks; retrying {} {} in 10s", count, clazz, saveResult.getId());
          Thread.sleep(10000);
          SaveResult retryResult = _update(count + 1, byId.get(saveResult.getId()))[0];
          saveResults[i] = retryResult;
        }
      }

      return saveResults;
    } catch (ConnectionException e) {
      log.warn("update attempt {} failed due to connection issues; retrying {} {} in 10s", count, clazz, ids, e);
      Thread.sleep(10000);
      return _update(count + 1, sObjects);
    }
  }

  private DeleteResult[] _delete(int count, SObject... sObjects) throws InterruptedException {
    String clazz = sObjects[0].getClass().getSimpleName();
    Map<String, SObject> byId = Arrays.stream(sObjects).collect(Collectors.toMap(SObject::getId, Function.identity()));
    String ids = byId.values().stream().map(SObject::getId).collect(Collectors.joining(","));

    if (count == 6) {
      log.error("unable to complete delete {} {} by attempt {}", clazz, ids, count);
      DeleteResult deleteResult = new DeleteResult();
      deleteResult.setSuccess(false);
      return new DeleteResult[]{deleteResult};
    }

    log.info("deleteing {} {}", clazz, ids);

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
          log.info("delete attempt {} failed due to locks; retrying {} {} in 10s", count, clazz, deleteResult.getId());
          Thread.sleep(10000);
          DeleteResult retryResult = _delete(count + 1, byId.get(deleteResult.getId()))[0];
          deleteResults[i] = retryResult;
        }
      }

      return deleteResults;
    } catch (ConnectionException e) {
      log.warn("delete attempt {} failed due to connection issues; retrying {} {} in 10s", count, clazz, ids, e);
      Thread.sleep(10000);
      return _delete(count + 1, sObjects);
    }
  }
}
