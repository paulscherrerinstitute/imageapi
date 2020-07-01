package ch.psi.daq.imageapi.generated;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;
import org.openapitools.jackson.nullable.JsonNullable;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * EventObject
 */

public class EventObject   {
  @JsonProperty("ts")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long ts;

  @JsonProperty("iocTs")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long iocTs;

  @JsonProperty("pulse")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long pulse;

  @JsonProperty("shape")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Valid
  private List<Integer> shape = null;

  public EventObject ts(Long ts) {
    this.ts = ts;
    return this;
  }

  /**
   * Get ts
   * @return ts
  */
  @ApiModelProperty(value = "")


  public Long getTs() {
    return ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }

  public EventObject iocTs(Long iocTs) {
    this.iocTs = iocTs;
    return this;
  }

  /**
   * Get iocTs
   * @return iocTs
  */
  @ApiModelProperty(value = "")


  public Long getIocTs() {
    return iocTs;
  }

  public void setIocTs(Long iocTs) {
    this.iocTs = iocTs;
  }

  public EventObject pulse(Long pulse) {
    this.pulse = pulse;
    return this;
  }

  /**
   * Get pulse
   * @return pulse
  */
  @ApiModelProperty(value = "")


  public Long getPulse() {
    return pulse;
  }

  public void setPulse(Long pulse) {
    this.pulse = pulse;
  }

  public EventObject shape(List<Integer> shape) {
    this.shape = shape;
    return this;
  }

  public EventObject addShapeItem(Integer shapeItem) {
    if (this.shape == null) {
      this.shape = new ArrayList<>();
    }
    this.shape.add(shapeItem);
    return this;
  }

  /**
   * Get shape
   * @return shape
  */
  @ApiModelProperty(value = "")


  public List<Integer> getShape() {
    return shape;
  }

  public void setShape(List<Integer> shape) {
    this.shape = shape;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EventObject eventObject = (EventObject) o;
    return Objects.equals(this.ts, eventObject.ts) &&
        Objects.equals(this.iocTs, eventObject.iocTs) &&
        Objects.equals(this.pulse, eventObject.pulse) &&
        Objects.equals(this.shape, eventObject.shape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ts, iocTs, pulse, shape);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EventObject {\n");
    
    sb.append("    ts: ").append(toIndentedString(ts)).append("\n");
    sb.append("    iocTs: ").append(toIndentedString(iocTs)).append("\n");
    sb.append("    pulse: ").append(toIndentedString(pulse)).append("\n");
    sb.append("    shape: ").append(toIndentedString(shape)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

