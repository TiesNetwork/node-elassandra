package network.tiesdb.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class Duration {

    private final BigDecimal value;

    public Duration(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal getInDecimal() {
        return value;
    }

    public BigInteger getInInteger() {
        return value.abs().toBigIntegerExact();
    }

    public BigDecimal getDecimal(DurationUnit unit) {
        return value.divide(unit.getValue(), RoundingMode.HALF_DOWN);
    }

    public BigInteger getInteger(DurationUnit unit) {
        return getDecimal(unit).toBigInteger();
    }

    public BigDecimal getPartDecimal(DurationUnit unit, DurationUnit per) {
        switch (unit.getValue().compareTo(per.getValue())) {
        case 0:
            return BigDecimal.ONE;
        case 1:
            return BigDecimal.ZERO;
        default:
            return value.divideAndRemainder(per.getValue())[1].divide(unit.getValue(), RoundingMode.HALF_DOWN);
        }
    }

    public BigInteger getPartInteger(DurationUnit unit, DurationUnit per) {
        return getPartDecimal(unit, per).toBigInteger();
    }

    public Duration add(Duration d) {
        return new Duration(value.add(d.value));
    }

    public Duration subtract(Duration d) {
        return new Duration(value.subtract(d.value));
    }

    public static interface Unit {

        String getName();

    }

    public static interface DurationUnit extends Unit {

        String getName();

        BigDecimal getValue();

    }

    public static enum DurationTimeUnit implements DurationUnit {

        NANOSECOND(1, -9), //
        MICROSECOND(1, -6), //
        MILLISECOND(1, -3), //
        SECOND(1, 0), //
        MINUTE(60, 0), //
        HOUR(60 * 60, 0), //
        DAY(60 * 60 * 24, 0), //

        ;

        private final BigDecimal value;

        private DurationTimeUnit(long multiplier, int scale) {
            this.value = new BigDecimal(BigInteger.valueOf(multiplier), scale * -1);
        }

        @Override
        public BigDecimal getValue() {
            return value;
        }

        @Override
        public String getName() {
            return name();
        }

    }

    @Override
    public String toString() {
        return "Duration [" + value + "s]";
    }

}