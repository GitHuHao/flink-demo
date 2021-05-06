package com.bigdata.flink.proj.testing.base;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

public class TestRideSource extends TestSource<TaxiRide> implements ResultTypeQueryable<TaxiRide> {
    public TestRideSource(Object ... eventsOrWatermarks) {
        this.testStream = eventsOrWatermarks;
    }

    @Override
    long getTimestamp(TaxiRide ride) {
        return ride.getEventTime();
    }

    @Override
    public TypeInformation<TaxiRide> getProducedType() {
        return TypeInformation.of(TaxiRide.class);
    }
}