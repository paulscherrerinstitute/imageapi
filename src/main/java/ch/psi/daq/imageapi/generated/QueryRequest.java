package ch.psi.daq.imageapi.generated;

import java.util.Objects;
import ch.psi.daq.imageapi.generated.QueryRequestDatetimerange;
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
 * QueryRequest
 */

public class QueryRequest   {
  @JsonProperty("channels")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Valid
  private List<String> channels = null;

  @JsonProperty("datetimerange")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private QueryRequestDatetimerange datetimerange;

  @JsonProperty("split")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer split = -1;

  @JsonProperty("snscount")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer snscount = -1;

  public QueryRequest channels(List<String> channels) {
    this.channels = channels;
    return this;
  }

  public QueryRequest addChannelsItem(String channelsItem) {
    if (this.channels == null) {
      this.channels = new ArrayList<>();
    }
    this.channels.add(channelsItem);
    return this;
  }

  /**
   * Get channels
   * @return channels
  */
  @ApiModelProperty(value = "")


  public List<String> getChannels() {
    return channels;
  }

  public void setChannels(List<String> channels) {
    this.channels = channels;
  }

  public QueryRequest datetimerange(QueryRequestDatetimerange datetimerange) {
    this.datetimerange = datetimerange;
    return this;
  }

  /**
   * Get datetimerange
   * @return datetimerange
  */
  @ApiModelProperty(value = "")

  @Valid

  public QueryRequestDatetimerange getDatetimerange() {
    return datetimerange;
  }

  public void setDatetimerange(QueryRequestDatetimerange datetimerange) {
    this.datetimerange = datetimerange;
  }

  public QueryRequest split(Integer split) {
    this.split = split;
    return this;
  }

  /**
   * Get split
   * @return split
  */
  @ApiModelProperty(value = "")


  public Integer getSplit() {
    return split;
  }

  public void setSplit(Integer split) {
    this.split = split;
  }

  public QueryRequest snscount(Integer snscount) {
    this.snscount = snscount;
    return this;
  }

  /**
   * Get snscount
   * @return snscount
  */
  @ApiModelProperty(value = "")


  public Integer getSnscount() {
    return snscount;
  }

  public void setSnscount(Integer snscount) {
    this.snscount = snscount;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryRequest queryRequest = (QueryRequest) o;
    return Objects.equals(this.channels, queryRequest.channels) &&
        Objects.equals(this.datetimerange, queryRequest.datetimerange) &&
        Objects.equals(this.split, queryRequest.split) &&
        Objects.equals(this.snscount, queryRequest.snscount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(channels, datetimerange, split, snscount);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class QueryRequest {\n");
    
    sb.append("    channels: ").append(toIndentedString(channels)).append("\n");
    sb.append("    datetimerange: ").append(toIndentedString(datetimerange)).append("\n");
    sb.append("    split: ").append(toIndentedString(split)).append("\n");
    sb.append("    snscount: ").append(toIndentedString(snscount)).append("\n");
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

