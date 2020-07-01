package ch.psi.daq.imageapi.generated;

import java.util.Objects;
import ch.psi.daq.imageapi.generated.ChannelData;
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
 * QueryResponse
 */

public class QueryResponse   {
  @JsonProperty("channels")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Valid
  private List<ChannelData> channels = null;

  @JsonProperty("something1")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String something1;

  public QueryResponse channels(List<ChannelData> channels) {
    this.channels = channels;
    return this;
  }

  public QueryResponse addChannelsItem(ChannelData channelsItem) {
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

  @Valid

  public List<ChannelData> getChannels() {
    return channels;
  }

  public void setChannels(List<ChannelData> channels) {
    this.channels = channels;
  }

  public QueryResponse something1(String something1) {
    this.something1 = something1;
    return this;
  }

  /**
   * Get something1
   * @return something1
  */
  @ApiModelProperty(value = "")


  public String getSomething1() {
    return something1;
  }

  public void setSomething1(String something1) {
    this.something1 = something1;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryResponse queryResponse = (QueryResponse) o;
    return Objects.equals(this.channels, queryResponse.channels) &&
        Objects.equals(this.something1, queryResponse.something1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(channels, something1);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class QueryResponse {\n");
    
    sb.append("    channels: ").append(toIndentedString(channels)).append("\n");
    sb.append("    something1: ").append(toIndentedString(something1)).append("\n");
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

