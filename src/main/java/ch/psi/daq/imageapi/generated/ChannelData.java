package ch.psi.daq.imageapi.generated;

import java.util.Objects;
import ch.psi.daq.imageapi.generated.EventObject;
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
 * ChannelData
 */

public class ChannelData   {
  @JsonProperty("channel")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String channel;

  @JsonProperty("events")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Valid
  private List<EventObject> events = null;

  public ChannelData channel(String channel) {
    this.channel = channel;
    return this;
  }

  /**
   * Get channel
   * @return channel
  */
  @ApiModelProperty(value = "")


  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public ChannelData events(List<EventObject> events) {
    this.events = events;
    return this;
  }

  public ChannelData addEventsItem(EventObject eventsItem) {
    if (this.events == null) {
      this.events = new ArrayList<>();
    }
    this.events.add(eventsItem);
    return this;
  }

  /**
   * Get events
   * @return events
  */
  @ApiModelProperty(value = "")

  @Valid

  public List<EventObject> getEvents() {
    return events;
  }

  public void setEvents(List<EventObject> events) {
    this.events = events;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChannelData channelData = (ChannelData) o;
    return Objects.equals(this.channel, channelData.channel) &&
        Objects.equals(this.events, channelData.events);
  }

  @Override
  public int hashCode() {
    return Objects.hash(channel, events);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ChannelData {\n");
    
    sb.append("    channel: ").append(toIndentedString(channel)).append("\n");
    sb.append("    events: ").append(toIndentedString(events)).append("\n");
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

