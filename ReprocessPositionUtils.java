
public class ReprocessPositionUtils {

    private ReprocessPositionUtils(){}


    public static Tuple2<Boolean, BigDecimal> frozenABForTriggeringReprocessOfUK(LiquidityScheduler liquidityScheduler, String key, boolean excludeReservedAmt, String requestId){

        AtomicReference<BigDecimal> atomicNewEarmarkingValue = new AtomicReference<>(null);
        AtomicReference<BigDecimal> atomicAvailableBalance = new AtomicReference<>(null);

		/******** Calling Utility CAS method to Update Position ********/
        boolean updatePosResult = HazelcastStore.casRetryPosition(key, oldValue -> {

            BigDecimal  availableBalance = getAvailableBalanceOfUK(liquidityScheduler, oldValue, excludeReservedAmt);
            logger.info("the Available Liquidity is : {}", availableBalance);

            if(availableBalance.compareTo(BigDecimal.ZERO) <= 0 ) {
                return null;
            }

            atomicAvailableBalance.set(availableBalance);

            //frozen position
            oldValue.setEarmarkingInTransitValue(getBigDecimalValue(oldValue.getEarmarkingInTransitValue()).subtract(availableBalance));
            atomicNewEarmarkingValue.set(oldValue.getEarmarkingInTransitValue());



            return oldValue;
        });

   
        return Tuple.of(updatePosResult, atomicAvailableBalance.get());
    }

    public static Tuple2<Boolean, BigDecimal> frozenABForTriggeringReprocessOfZA(String key, String requestId){
        AtomicReference<BigDecimal> atomicNewEarmarkingValue = new AtomicReference<>(null);
        AtomicReference<BigDecimal> atomicAvailableBalance = new AtomicReference<>(null);

		/******** Calling Utility CAS method to Update Position ********/
        boolean updatePosResult = HazelcastStore.casRetryPosition(key, oldValue -> {

            BigDecimal availableBalance = getAvailableBalanceOfZA(oldValue);
            logger.info("the Available Liquidity is : {}", availableBalance);

            if(availableBalance.compareTo(BigDecimal.ZERO) <= 0 ) {
                return null;
            }

            atomicAvailableBalance.set(availableBalance);

            //frozen position
            oldValue.setEarmarkingInTransitValue(ReprocessPositionUtils.getBigDecimalValue(oldValue.getEarmarkingInTransitValue()).add(availableBalance));

            atomicNewEarmarkingValue.set(oldValue.getEarmarkingInTransitValue());

            logger.info("Position frozen in process, officeKey: {}, requestId: {} ,availableBalance: {}, newEarmarking:{}",
                    key, requestId, atomicAvailableBalance.get(), atomicNewEarmarkingValue.get());

            return oldValue;
        });



        return Tuple.of(updatePosResult, atomicAvailableBalance.get());
    }


}
