package com.oner.model;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
public class BiTemporalDoc implements Serializable {
    private TemporalHeader temporalData;
    private LogicalId logicalId;

    private LocalDateTime transactionTime;

    private String currency;
    private LocalDate maturityDate;
    private double currentCoupon;

    @Data
    public static class LogicalId implements Serializable {
        private final String source;
        private final String dataId;
    }

    @Data
    public static class TemporalHeader implements Serializable {
        private LocalDateTime from;
        private LocalDateTime to;
    }


    public boolean isValid(LocalDateTime asAtDate) {
        return asAtDate.isAfter(getTemporalData().from) && asAtDate.isBefore(getTemporalData().to);
    }

    public boolean isValidTransaction(LocalDateTime asAtDate) {
        return transactionTime.compareTo(asAtDate) >= 0;
    }
}

