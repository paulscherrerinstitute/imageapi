package ch.psi.daq.imageapi3;

import java.time.Instant;

public class DateRange extends Range {
    private Instant startDate;
    private Instant endDate;

    public Instant getStartDate() {
        return startDate;
    }

    public void setStartDate(Instant startDate) {
        this.startDate = startDate;
    }

    public Instant getEndDate() {
        return endDate;
    }

    public void setEndDate(Instant endDate) {
        this.endDate = endDate;
    }
}
