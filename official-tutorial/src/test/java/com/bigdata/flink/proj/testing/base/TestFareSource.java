package com.bigdata.flink.proj.testing.base;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

public class TestFareSource extends TestSource<TaxiFare> implements ResultTypeQueryable<TaxiFare> {
    public TestFareSource(Object ... eventsOrWatermarks) {
        this.testStream = eventsOrWatermarks;
    }

    @Override
    long getTimestamp(TaxiFare fare) {
        return fare.getEventTime();
    }

    @Override
    public TypeInformation<TaxiFare> getProducedType() {
        return TypeInformation.of(TaxiFare.class);
    }
}