package ch.psi.daq.imageapi3;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChannelConfig {

    private String channelName;
    private Instant timestamp;
    private long pulseId;
    private int keyspace;
    private long binSize;
    private int splitCount;
    private int status; // status is a bitset: 1bit=IS_STATISTICS_PRECOMPUTED 2bit=IS_LOCAL_WRITE 3 bit=IS_OPTIONAL_EVENT_FIELDS //  status = 1 << bit_number
    private byte backendByte;
    private int modulo;
    private int offset;
    private short precision; // DEFAULT_FLOAT32_PRECISION = -7 / DEFAULT_FLOAT64_PRECISION = -16 / DEFAULT_PRECISION = 0 / see: https://stackoverflow.com/a/11825877
    private DType dtype;
    private String source;
    private String unit;
    private String description;
    private String optionalFields;
    private String valueConverter;

    public ChannelConfig(){
    }

    public ChannelConfig(String channelName, Instant timestamp, long pulseId, int keyspace, long binSize, int splitCount, int status, byte backendByte, int modulo, int offset, short precision, DType dtype, String source, String unit, String description, String optionalFields, String valueConverter) {
        this.channelName = channelName;
        this.timestamp = timestamp;
        this.pulseId = pulseId;
        this.keyspace = keyspace;
        this.binSize = binSize;
        this.splitCount = splitCount;
        this.status = status;
        this.backendByte = backendByte;
        this.modulo = modulo;
        this.offset = offset;
        this.precision = precision;
        this.dtype = dtype;
        this.source = source;
        this.unit = unit;
        this.description = description;
        this.optionalFields = optionalFields;
        this.valueConverter = valueConverter;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public long getPulseId() {
        return pulseId;
    }

    public void setPulseId(long pulseId) {
        this.pulseId = pulseId;
    }

    public int getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(int keyspace) {
        this.keyspace = keyspace;
    }

    public long getBinSize() {
        return binSize;
    }

    public void setBinSize(long binSize) {
        this.binSize = binSize;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public byte getBackendByte() {
        return backendByte;
    }

    public void setBackendByte(byte backendByte) {
        this.backendByte = backendByte;
    }

    public int getModulo() {
        return modulo;
    }

    public void setModulo(int modulo) {
        this.modulo = modulo;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public short getPrecision() {
        return precision;
    }

    public void setPrecision(short precision) {
        this.precision = precision;
    }

    public DType getDtype() {
        return dtype;
    }

    public void setDtype(DType dtype) {
        this.dtype = dtype;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOptionalFields() {
        return optionalFields;
    }

    public void setOptionalFields(String optionalFields) {
        this.optionalFields = optionalFields;
    }

    public String getValueConverter() {
        return valueConverter;
    }

    public void setValueConverter(String valueConverter) {
        this.valueConverter = valueConverter;
    }
}
