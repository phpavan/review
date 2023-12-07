
public class ReprocessHelper {

    private static final Logger logger = LogManager.getLogger(ReprocessHelper.class);

    private ReprocessHelper(){}

    public static void ukReprocessBlocAndHeldPayments(String eventId, String operator, String officeKey, List<String> counterpartyBICList) {
        logger.info("try reprocess UK Bloc / Held payments");

        String schedulerKey = HZUtils.getSchedulerKey(officeKey);
		/**HazelCast AtomicReference READ -Scheduler ***/
        LiquidityScheduler liquidityScheduler = HazelcastStore.getLiquidityScheduler(schedulerKey);
        if (liquidityScheduler == null) {
            throw new HazelcastStoreException( new Exception("Failed to load Scheduler from HazelCast, officeKey: " + officeKey));

        }
        String positionKey = HZUtils.getPositionKey(officeKey);
		/**HazelCast AtomicReference READ -position ***/
        LiquidityPositionDetail positionDetail = HazelcastStore.getLiquidityPositionDetail(positionKey);
        if (positionDetail == null) {
            throw new HazelcastStoreException( new Exception("Failed to load Position from HazelCast, officeKey: " + officeKey));
        }


        //check if able to reprocess
        boolean autoReprocess = ReprocessPositionUtils.judgeAutoReprocessUK(liquidityScheduler, positionDetail, true);
        logger.info("auto reprocess cond pre-check : {}", autoReprocess);

        Tuple2<Boolean, BigDecimal> frozenPositionResult = null;
        if (autoReprocess) {
            logger.info("try to frozen Earmarking position for reprocess");
			
			/**HazelCast AtomicReference CAS Update -position ***/
            frozenPositionResult = ReprocessPositionUtils.frozenABForTriggeringReprocessOfUK(liquidityScheduler,positionKey,excludeReservedAmt, eventId);
        }
		
        //Sent to Kafka to trigger reprocess 
        LiquidityEvent liquidityEvent = new LiquidityEvent();
        ReprocessEventSender.getInstance().sendAutoReprocessEvent(partition, liquidityEvent, eventId);
        



    }

    public static void zaReprocessPayments(String eventId, String operator, String officeKey) {
        logger.info("try reprocess ZA payments");

        String schedulerKey = HZUtils.getPositionKey(officeKey);
		/**HazelCast AtomicReference READ -Scheduler ***/
        LiquidityScheduler liquidityScheduler = HazelcastStore.getLiquidityScheduler(schedulerKey);
        if (liquidityScheduler == null) {
            throw new HazelcastStoreException( new Exception("Failed to load Scheduler from HazelCast, officeKey: " + officeKey));

        }
        String positionKey = HZUtils.getPositionKey(officeKey);
		/**HazelCast AtomicReference READ -position ***/
        LiquidityPositionDetail positionDetail = HazelcastStore.getLiquidityPositionDetail(positionKey);
        if (positionDetail == null) {
            throw new HazelcastStoreException( new Exception("Failed to load Position from HazelCast, officeKey: " + officeKey));
        }

        //check if able to reprocess
        boolean autoReprocess = ReprocessPositionUtils.judgeAutoReprocessZA(liquidityScheduler, positionDetail);
        logger.info("auto reprocess cond: {}", autoReprocess);

        Tuple2<Boolean, BigDecimal> frozenPositionResult = null;
        if (autoReprocess) {
            logger.info("try to frozen Earmarking position for reprocess");
			/**HazelCast AtomicReference READ -Scheduler ***/
            frozenPositionResult = ReprocessPositionUtils.frozenABForTriggeringReprocessOfZA(positionKey, eventId);
        }

		//Sent to Kafka to trigger reprocess 
        LiquidityEvent liquidityEvent = new LiquidityEvent();
        ReprocessEventSender.getInstance().sendAutoReprocessEvent(partition, liquidityEvent, eventId);
        



    }
}
