package ch.psi.daq.imageapi.generated;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.openapitools.jackson.nullable.JsonNullable;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * QueryRequestDatetimerange
 */

public class QueryRequestDatetimerange   {
  @JsonProperty("begin")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String begin;

  @JsonProperty("end")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String end;

  public QueryRequestDatetimerange begin(String begin) {
    this.begin = begin;
    return this;
  }

  /**
   * Get begin
   * @return begin
  */
  @ApiModelProperty(value = "")


  public String getBegin() {
    return begin;
  }

  public void setBegin(String begin) {
    this.begin = begin;
  }

  public QueryRequestDatetimerange end(String end) {
    this.end = end;
    return this;
  }

  /**
   * Get end
   * @return end
  */
  @ApiModelProperty(value = "")


  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryRequestDatetimerange queryRequestDatetimerange = (QueryRequestDatetimerange) o;
    return Objects.equals(this.begin, queryRequestDatetimerange.begin) &&
        Objects.equals(this.end, queryRequestDatetimerange.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(begin, end);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class QueryRequestDatetimerange {\n");
    
    sb.append("    begin: ").append(toIndentedString(begin)).append("\n");
    sb.append("    end: ").append(toIndentedString(end)).append("\n");
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

