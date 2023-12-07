

public class Utils {


    private Utils(){
    }

    public static String tryLock(FencedLock lock, String transactionId){
        logger.info("start to lock payment, transaction id: {}", transactionId);
        String result = null;
        try {
            result = lock.tryLock() ? Constants.RESULT_SUCCESS : Constants.RESULT_FAILED;
        }catch (DistributedObjectDestroyedException e){
            result = Constants.RESULT_DESTROY;
            logger.warn("Encountered lock concurrency problems when executing tryLock, the currently used lock may have been destroyed. transaction id: {}, Exception details:[{}]", transactionId, ExceptionUtils.getStackTrace(e));
        }
        return result;
    }

    public static String unlock(FencedLock lock, String transactionId){
        logger.info("start to unlock payment, transaction id: {}", transactionId);
        try {
            lock.unlock();
            return Constants.RESULT_SUCCESS;
        }catch (DistributedObjectDestroyedException e){
            logger.warn("Encountered lock concurrency problems when executing unlock, the currently used lock may have been destroyed, transaction id: {}, Exception details:[{}]", transactionId, ExceptionUtils.getStackTrace(e));
            return Constants.RESULT_DESTROY;
        }catch (Exception e){
            logger.warn("hit exception when unlock payment, transaction id: {}, exception: {}", transactionId, ExceptionUtils.getStackTrace(e));
            return Constants.RESULT_FAILED_EXCEPTION;
        }
    }


    public static LiquidityScheduler getLiquidityScheduler(String officeKey) {
        String dataKey = Constants.LIQUIDITY_SCHEDULER_PREFIX + officeKey;
        return  HazelcastStore.getLiquidityScheduler(dataKey);
    }

    public static LiquidityPositionDetail getLiquidityPositionDetail(String officeKey) {
        String dataKey = Constants.LIQUIDITY_POSITION_PREFIX + officeKey;
        return  HazelcastStore.getLiquidityPositionDetail(dataKey);
    }

   

}
