
public class HZSchedulerHelper {

    private static final Logger logger = LogManager.getLogger(HZSchedulerHelper.class);

    private String mapKeyScheduler = null;
    private List<String> apSchedulerKeyLst = null;

    private HZCommonHelper hzCommonHelper;

    private HZSchedulerHelper() {
        mapKeyScheduler = HZUtils.loadEnvVariableOrDefault("IMAP_KEY_SCHEDULER", String.class, "pol.liquidity-scheduler");

		/**
		 * **** Currently not open for use, for consistence issue among IMAP & AtomicReference ***
		 * Scheduler that require read-write segregation. 
		 * Read :  Read from IMAP.  if not exists, read CP AtomicReference
		 * Write:  Write to CP, Sync to IMAP 
		*/
        //liquidity-scheduler-GB-HBUK-STG-GBP,liquidity-scheduler-GB-MIDL-STG-GBP
        String apScheudlerKey = HZUtils.loadEnvVariable("AP_SCHEDULER_LIST", String.class);
        if(StringUtils.isNotBlank(apScheudlerKey)) {
            apSchedulerKeyLst = Arrays.asList(apScheudlerKey.split(","));
        } else {
            apSchedulerKeyLst = new ArrayList<>(0);
        }

    }

    private static HZSchedulerHelper instance ;
    public static synchronized  HZSchedulerHelper getInstance(HZCommonHelper commHelper){
        if (instance == null) {
            instance = new HZSchedulerHelper();
            instance.hzCommonHelper = commHelper;
        }

        return instance;
    }

    /**
     * for AP scheduler key, read from IMAP as priority, if fail, then read from AtomicRef
     * @param dataKey
     * @return
     */
    public LiquidityScheduler getLiquidityScheduler(String dataKey) {
        logger.info("start to get scheduler from Hazelcast with key: {}", dataKey);

        if (StringUtils.isBlank(dataKey) || (HazelcastStore.getHazelcastCPSubsystem() == null)) {
            logger.error("failed to get scheduler from Hazelcast: invalid key or Hazelcast is not initialized, key is: {}", dataKey);
            return null;
        }

        //1. try read from IMAP
        boolean isAPSchedulerSupportedKey = isAPSupportedKey(dataKey);
        if (isAPSchedulerSupportedKey) {
            logger.info("AP Scheduler supported key, read data from IMAP. key:{}", dataKey);
            LiquidityScheduler liquidityScheduler = getSchedulerFromIMap(dataKey);
            if (liquidityScheduler != null) {
                logger.info("get Scheduler from IMAP, key:{}, value:{}", dataKey, liquidityScheduler);
                return liquidityScheduler;
            } else {
                logger.info("Scheduler not exist in IMAP. key:{}", dataKey);
            }
        }

        //2. Read from CP system AtomicRef
        logger.info("Read data from CP system. key:{}", dataKey);
        String dataValue = HazelcastStore.getJsonMsg(dataKey);
        LiquidityScheduler liquidityScheduler = parseSchedulerString(dataValue);
        if(liquidityScheduler == null) {
            logger.error("failed to get scheduler from Hazelcast: key {}", dataKey);
            return null;
        }

        //3. Sync CP scheduler data to IMAP
        if(isAPSchedulerSupportedKey) {
            String convertedDataValue = HZUtils.convertJsonStringToSingleLine(dataValue);
            logger.info("AP Scheduler supported key, sync CP data to IMAP. key:{}, value:{}", dataKey, convertedDataValue);

            setScheduleToIMap(dataKey, liquidityScheduler);
        }

        return liquidityScheduler;
    }


	/**
	 *  Update Scheduler
	*/
    public  boolean casRetryScheduler(String key, UnaryOperator<LiquidityScheduler> funcBizUpdate,  boolean retry) {
		//If ReadWrite Segregation supported, update Imap & CP 
        if (isAPSupportedKey(key)){
            return casRetrySchedulerAPCP(key, funcBizUpdate, retry);
        } else {
			
			//non ReadWrite Segregation supported, update CP only 
            return casRetrySchedulerCP(key, funcBizUpdate, retry);
        }

    }

    /**
     * Non-AP scheduler Key, will update CP atomicRef only
     * @param key
     * @param funcBizUpdate  business logic 
     * @param retry
     * @return
     */
    private boolean casRetrySchedulerCP(String key, UnaryOperator<LiquidityScheduler> funcBizUpdate, boolean retry) {
        logger.info("update Scheduler to CP atomicRef.");
        try {

            Boolean retryResult = hzCommonHelper.retryLogic(key, reference -> {
                Gson gson = GsonUtils.getUpperCamelGson();

                String oldSchedulerStr = HZUtils.getStringValueFromAtomRef(key ,reference.get());
                LiquidityScheduler oldScheduler = gson.fromJson(oldSchedulerStr, LiquidityScheduler.class);

				//apply business changes to gen a new image
                LiquidityScheduler newScheduler = funcBizUpdate.apply(oldScheduler);
                String newSchedulerStr = gson.toJson(newScheduler);


                Object oldData = HZUtils.toAtomRefData(key,  hzCommonHelper.isCompressedKey(key),oldSchedulerStr);
                Object newData = HZUtils.toAtomRefData(key,  hzCommonHelper.isCompressedKey(key),newSchedulerStr);
                return reference.compareAndSet(oldData, newData);

            },retry);

            logger.info("update Scheduler to CP atomicRef result: {}" , retryResult);
            return retryResult ;
        } catch (Exception ex) {
            logger.error("casRetryScheduler fails to update with key: {}, exception: {}", key, ex.toString());
            throw new HazelcastStoreException(ex);
        }
    }

    /**
     * Scheduler update in both AP IMAP & CP atomicRef
     * @param key
     * @param funcBizUpdate
     * @param retry
     * @return
     */
    private boolean casRetrySchedulerAPCP(String key, UnaryOperator<LiquidityScheduler> funcBizUpdate, boolean retry) {
        logger.info("update Scheduler to IMap and CP atomicRef.");
        try {

            Boolean retryResult = hzCommonHelper.retryLogic(key, reference -> {
                Gson gson = GsonUtils.getUpperCamelGson();

                String oldSchedulerStr = HZUtils.getStringValueFromAtomRef(key ,reference.get());
                LiquidityScheduler oldScheduler = gson.fromJson(oldSchedulerStr, LiquidityScheduler.class);

                LiquidityScheduler newScheduler = funcBizUpdate.apply(oldScheduler);
                String newSchedulerStr = gson.toJson(newScheduler);


                //1. update IMAP
                logger.info("Start update Scheduler to IMap.");
                setScheduleToIMap(key,newScheduler);

                //2. Update AtomicRef
                boolean updateResult;
                try{
                    Object oldData = HZUtils.toAtomRefData(key,  hzCommonHelper.isCompressedKey(key),oldSchedulerStr);
                    Object newData = HZUtils.toAtomRefData(key,  hzCommonHelper.isCompressedKey(key),newSchedulerStr);

                    logger.info("update Scheduler to CP atomicRef");
                    updateResult =  reference.compareAndSet(oldData, newData);

                    if(!updateResult){
                        logger.info("update Scheduler to CP atomicRef failed, will rollback IMap Scheduler.");
                        //3. roll back imap if AtomicRef failed
                        boolean rollBackImapSchedulerWhenCpFail = true;
                        if (rollBackImapSchedulerWhenCpFail){
                            logger.info("Rolling back IMAP. key:{}", key);
                            setScheduleToIMap(key,oldScheduler);
                        }

                    } else {
                        //log
                        String convertedNewValueStr = HZUtils.convertJsonStringToSingleLine(newSchedulerStr);
                        logger.info("update CP scheduler success: key = {}, value = {}", key, convertedNewValueStr);
                    }

                }catch (Exception e){
                    logger.error("failed to update CP scheduler with key {}, exception {}", key, ExceptionUtils.getStackTrace(e));

                    //3. roll back imap if AtomicRef failed
                    boolean rollBackImapSchedulerWhenCpFail = true;
                    if (rollBackImapSchedulerWhenCpFail){
                        logger.info("Rolling back IMAP. key:{}", key);
                        setScheduleToIMap(key,oldScheduler);
                    }

                    throw e;
                }

                return updateResult;
            },retry);

            return retryResult ;
        } catch (Exception ex) {
            logger.error("casRetryScheduler fails to update with key: {}, exception: {}", key, ex.toString());
            throw new HazelcastStoreException(ex);
        }
    }

    //pol.liquidity-scheduler
    private IMap<String, byte[]> getIMap(){
        String realKey = mapKeyScheduler;
        IMap<String, byte[]> schedulerImap = HazelcastStore.getHazelcastInstance().getMap(realKey);
        return schedulerImap;

    }

    private LiquidityScheduler getSchedulerFromIMap(String scheduleKey) {
        IMap<String, byte[]> schedulerImap = getIMap();

        byte[] dataObj = schedulerImap.get(scheduleKey);

        String schedulerStr = HZUtils.getStringValueFromAtomRef(scheduleKey, dataObj);

        if(StringUtils.isBlank(schedulerStr)) {
            return null;
        }

        return GsonUtils.getUpperCamelGson().fromJson(schedulerStr, LiquidityScheduler.class);
    }

    public void setScheduleToIMap(String scheduleKey, LiquidityScheduler data) {
        IMap<String, byte[]> schedulerImap = getIMap();

        //compress value obj to byte array.
        String schedulerStr = GsonUtils.getUpperCamelGson().toJson(data);
        byte[] bytesScheduler = HZUtils.compressHazelCastValue(scheduleKey, schedulerStr);

        schedulerImap.set(scheduleKey, bytesScheduler);
    }


    private boolean isAPSupportedKey(String dataKey){
        return apSchedulerKeyLst.contains(dataKey);
    }

    private LiquidityScheduler parseSchedulerString(String value){
        if (StringUtils.isBlank(value)) {
            logger.error("scheduler from Hazelcast is blank with key {}", value);
            return null;
        }

        try {
            LiquidityScheduler liquidityScheduler = GsonUtils.getUpperCamelGson().fromJson(value, LiquidityScheduler.class);
            return liquidityScheduler;

        } catch (Exception e){
            //JsonSyntaxException
            logger.error("failed to parse scheduler data with exception {}, original data:{}", ExceptionUtils.getStackTrace(e), value);
            return null;
        }

    }

}

