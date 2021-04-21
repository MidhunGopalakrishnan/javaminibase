package global;

/**
 * Enumeration class for AggType
 *
 */

public class AggType {

    public static final int aggMax    = 0;
    public static final int aggMin    = 1;
    public static final int aggAvg    = 2;
    public static final int aggSky    = 3;

    public int aggType;

    /**
     * AggType Constructor
     * <br>
     * An aggregation type of max can be defined as
     * <ul>
     * <li>   AggType aggType = new AggType(AggType.aggMax);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (aggType.aggType == AggType.aggMax) ....
     * </ul>
     *
     * @param _aggType The types of attributes available in this class
     */

    public AggType(int _aggType) {
        aggType = _aggType;
    }

    public String toString() {

        switch (aggType) {
            case aggMax:
                return "maximum";
            case aggMin:
                return "minimum";
            case aggAvg:
                return "average";
            case aggSky:
                return "skyline";
        }
        return ("Unexpected AggType " + aggType);
    }
}
