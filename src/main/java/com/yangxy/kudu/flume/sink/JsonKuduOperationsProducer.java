package com.yangxy.kudu.flume.sink;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.flume.sink.KuduOperationsProducer;
import org.apache.kudu.flume.sink.RegexpKuduOperationsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;


/**
 * A json operations producer that generates one or more Kudu
 * {@link Insert} or {@link Upsert} operations per Flume {@link Event} by
 * parsing the event {@code body} using a regular expression. Values are
 * coerced to the types of the named columns in the Kudu table.
 * <p>
 * <p>Example: If the Kudu table has the schema:
 * <p>
 * <pre>
 * key INT32
 * name STRING</pre>
 * <p>
 * <p>and {@code producer event body like {"key":"value_of_key","name":"the_value_of_name"}} :
 * <p>
 * <pre>|12345,Mike||54321,Todd|</pre>
 *
 * into the rows: {@code (key=12345, name=Mike)} and {@code (key=54321, name=Todd)}.
 *
 * <p>Note: This class relies on JDK7 named capturing groups, which are
 * documented in {@link Pattern}. The name of each capturing group must
 * correspond to a column name in the destination Kudu table.
 *
 * <p><strong><code>JsonKuduOperationsProducer</code> Flume Configuration Parameters</strong></p>
 *
 * <table cellpadding=3 cellspacing=0 border=1 summary="Flume Configuration Parameters">
 * <tr>
 * <th>Property Name</th>
 * <th>Default</th>
 * <th>Required?</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>producer.skipErrorEvent</td>
 * <td>true</td>
 * <td>No</td>
 * <td>The regular expression used to parse the event body.</td>
 * </tr>
 * <tr>
 * <td>producer.charset</td>
 * <td>utf-8</td>
 * <td>No</td>
 * <td>The character set of the event body.</td>
 * </tr>
 * <tr>
 * <td>producer.operation</td>
 * <td>upsert</td>
 * <td>No</td>
 * <td>Operation type used to write the event to Kudu. Must be either
 * {@code insert} or {@code upsert}.</td>
 * </tr>
 * <tr>
 * <td>producer.skipMissingColumn</td>
 * <td>false</td>
 * <td>No</td>
 * <td>What to do if a column in the Kudu table has no corresponding capture group.
 * If set to {@code true}, a warning message is logged and the operation is still attempted.
 * If set to {@code false}, an exception is thrown and the sink will not process the
 * {@code Event}, causing a Flume {@code Channel} rollback.
 * </tr>
 * <tr>
 * <td>producer.skipBadColumnValue</td>
 * <td>false</td>
 * <td>No</td>
 * <td>What to do if a value in the pattern match cannot be coerced to the required type.
 * If set to {@code true}, a warning message is logged and the operation is still attempted.
 * If set to {@code false}, an exception is thrown and the sink will not process the
 * {@code Event}, causing a Flume {@code Channel} rollback.
 * </tr>
 * <tr>
 * <td>producer.kuduTimeStampColums</td>
 * <td>null</td>
 * <td>No</td>
 * <td>Comma Separated Kudu TimeStamp Columes</td>
 * </tr>
 * <tr>
 * <td>producer.inputDateFormat</td>
 * <td>null</td>
 * <td>No</td>
 * <td>the data for Kudu TimeStamp Columes input format</td>
 * </tr>
 * </table>
 *
 * @see Pattern
 */
public class JsonKuduOperationsProducer implements KuduOperationsProducer {

    private static final Logger logger = LoggerFactory.getLogger(RegexpKuduOperationsProducer.class);
    private static final String INSERT = "insert";
    private static final String UPSERT = "upsert";
    private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);


    public static final String ENCODING_PROP = "encoding";
    public static final String DEFAULT_ENCODING = "utf-8";
    public static final String OPERATION_PROP = "operation";
    public static final String DEFAULT_OPERATION = UPSERT;

    public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
    public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = false;

    public static final String SKIP_ERROR_EVENT_PROP = "skipErrorEvent";
    public static final boolean DEFAULT_SKIP_ERROR_EVENT = false;

    public static final String ADD_CONTENT_MD5 = "addContentMD5";
    public static final String ADD_TIMESTAMP_COLUM = "addTimeStampColum";

    public static final String KUDU_TIMESTAMP_COLUMS = "kuduTimeStampColums";

    //case sensitive
    public static final String KUDU_PRIMARY_KEYS = "jsonPrimaryKeys";
    public static final String INPUT_DATE_FORMAT = "inputDateFormat";
    public static final String DEFAULT_INPUT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.S";

    private Set<String> primaryKeySet = null;


    private KuduTable table;

    private Charset charset;
    private String operation;
    private boolean skipBadColumnValue;
    private boolean skipErrorEvent;
    private Set<String> timeColumSet;
    private DateFormat dateFormat;
    private boolean addContentMD5;
    private Boolean addTimeStamp;

    @Override
    public void configure(Context context) {

        String charsetName = context.getString(ENCODING_PROP, DEFAULT_ENCODING);
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException e) {
            throw new FlumeException(
                    String.format("Invalid or unsupported charset %s", charsetName), e);
        }
        operation = context.getString(OPERATION_PROP, DEFAULT_OPERATION).toLowerCase();
        Preconditions.checkArgument(
                validOperations.contains(operation),
                "Unrecognized operation '%s'",
                operation);
        skipErrorEvent = context.getBoolean(SKIP_ERROR_EVENT_PROP,
                DEFAULT_SKIP_ERROR_EVENT);
        skipBadColumnValue = context.getBoolean(SKIP_BAD_COLUMN_VALUE_PROP,
                DEFAULT_SKIP_BAD_COLUMN_VALUE);

        addContentMD5 = context.getBoolean(ADD_CONTENT_MD5, true);
        addTimeStamp = context.getBoolean(ADD_TIMESTAMP_COLUM, true);

        String primaryKeys = context.getString(KUDU_PRIMARY_KEYS, "");
        primaryKeySet = new HashSet<>();
        String[] pks = primaryKeys.split(",");
        for (String pk : pks) {
            if (!"".equals(pk.trim())) {
                primaryKeySet.add(pk.trim());
            }
        }

        String inputDateFormat = context.getString(INPUT_DATE_FORMAT, DEFAULT_INPUT_DATE_FORMAT);
        dateFormat = new SimpleDateFormat(inputDateFormat);

        String timeColumes = context.getString(KUDU_TIMESTAMP_COLUMS);
        if (timeColumes != null && !StringUtils.isEmpty(timeColumes)) {
            String msg = "kuduTimeStampColums=%s,inputDateFormat=%s";
            logger.info(String.format(msg, timeColumes, inputDateFormat));
            String[] array = timeColumes.split(",");
            timeColumSet = new HashSet<>();
            for (String col : array) {
                timeColumSet.add(StringUtils.trim(col));
            }
        }
    }

    @Override
    public void initialize(KuduTable table) {
        this.table = table;
    }

    @Override
    public List<Operation> getOperations(Event event) throws FlumeException {
        List<Operation> ops = Lists.newArrayList();
        String raw = new String(event.getBody(), charset);
        try {
            JSONObject json = JSONObject.parseObject(raw);
            if (addContentMD5) {
                json.put("md5", DigestUtils.md5Hex(raw));
            }
            if (addTimeStamp) {
                json.put("timestamp", currentUnixTimeMicros());
            }
            Schema schema = table.getSchema();
            if (!json.isEmpty()) {
                Operation op = getOperation(raw, json, schema);
                ops.add(op);
            }
        } catch (JSONException e) {
            String msg = String.format(
                    "Event '%s' Can't parse to Json Object", raw);
            logOrThrow(skipErrorEvent, msg, e);
        }

        return ops;
    }

    private long currentUnixTimeMicros() {
        return System.currentTimeMillis() * 1000;
    }

    private Operation getOperation(String raw, JSONObject json, Schema schema) {
        Operation op = getOperationType();
        ColumnSchema col = null;
        PartialRow row = op.getRow();
        for (String primary : primaryKeySet) {
            Object val = json.get(primary);
            if (val == null) {
                json.put(primary, "");
            }
        }
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            try {
                String key = entry.getKey();
                col = schema.getColumn(key.toLowerCase());
                String value = null;
                if (entry.getValue() != null) {
                    value = entry.getValue().toString();
                }
                if (timeColumSet != null && timeColumSet.contains(col.getName())) {
                    value = toUnixtimeMicros(value);
                    //Bigger than current Time 2 hours
                    long hours = currentUnixTimeMicros() + 2 * 60 * 3600 * 1000 * 1000L;
                    if (Long.valueOf(value) > hours) {
                        logger.warn(String.format("%s Bigger than current Time %s  2 hours: %s",
                                col.getName(), Long.toString(hours), json.toJSONString()));
                        value = Long.toString(currentUnixTimeMicros());
                    }
                }
                if (!isNullOrEmpty(value)) {
                    coerceAndSet(value, col.getName(), col.getType(), row);
                }
                if (isNullOrEmpty(value) && isPrimaryKey(key)) {
                    coerceAndSet(col.getName(), col.getType(), row);
                }
            } catch (NumberFormatException e) {
                String msg = String.format(
                        "Raw value '%s' couldn't be parsed to type %s for column '%s'",
                        raw, col.getType(), col.getName());
                logOrThrow(skipBadColumnValue, msg, e);
            } catch (Exception e) {
                throw new FlumeException("Failed to create Kudu operation", e);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Operation: " + op.getRow().toString());
        }
        return op;
    }

    private String toUnixtimeMicros(String dateString) throws ParseException {
        if (dateString == null || "".equals(dateString.trim())) {
            return dateString;
        }
        Date date = dateFormat.parse(dateString);
        long micros = date.getTime() * 1000;
        return Long.toString(micros);
    }

    private Operation getOperationType() {
        Operation op;
        switch (operation) {
            case UPSERT:
                op = table.newUpsert();
                break;
            case INSERT:
                op = table.newInsert();
                break;
            default:
                throw new FlumeException(
                        String.format("Unrecognized operation type '%s' in getOperations(): " +
                                "this should never happen!", operation));
        }
        return op;
    }


    /**
     * Coerces the string `rawVal` to the type `type` and sets the resulting
     * value for column `colName` in `row`.
     *
     * @param rawVal  the raw string column value
     * @param colName the name of the column
     * @param type    the Kudu type to convert `rawVal` to
     * @param row     the row to set the value in
     * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
     */
    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
            throws NumberFormatException {
        switch (type) {
            case INT8:
                row.addByte(colName, Byte.parseByte(rawVal));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rawVal));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rawVal));
                break;
            case INT64:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            case BINARY:
                row.addBinary(colName, rawVal.getBytes(charset));
                break;
            case STRING:
                row.addString(colName, rawVal);
                break;
            case BOOL:
                row.addBoolean(colName, Boolean.parseBoolean(rawVal));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rawVal));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rawVal));
                break;
            case UNIXTIME_MICROS:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column",
                        type, colName);
        }
    }


    private void coerceAndSet(String colName, Type type, PartialRow row)
            throws NumberFormatException {
        switch (type) {
            case INT8:
                row.addByte(colName, (byte) -1);
                break;
            case INT16:
                row.addShort(colName, (short) -1);
                break;
            case INT32:
                row.addInt(colName, -1);
                break;
            case INT64:
                row.addLong(colName, -1);
                break;
            case BINARY:
                row.addBinary(colName, "".getBytes(charset));
                break;
            case STRING:
                row.addString(colName, "");
                break;
            case BOOL:
                row.addBoolean(colName, false);
                break;
            case FLOAT:
                row.addFloat(colName, -1);
                break;
            case DOUBLE:
                row.addDouble(colName, -1);
                break;
            case UNIXTIME_MICROS:
                row.addLong(colName, currentUnixTimeMicros());
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column",
                        type, colName);
        }
    }

    private void logOrThrow(boolean log, String msg, Exception e)
            throws FlumeException {
        if (log) {
            logger.warn(msg, e);
        } else {
            throw new FlumeException(msg, e);
        }
    }

    private boolean isNullOrEmpty(String value) {
        if (value == null || "".equals(value.trim()) || "null".equals(value.trim().toLowerCase())) {
            return true;
        }
        return false;
    }

    private boolean isPrimaryKey(String key) {
        return primaryKeySet.contains(key);
    }

    @Override
    public void close() {
    }
}
