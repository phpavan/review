
public class HZPositionHelper {

    private static final Logger logger = LogManager.getLogger(HZPositionHelper.class);

    private HZCommonHelper hzCommonHelper;

    private HZPositionHelper() {

    }

    private static HZPositionHelper instance ;
    public static synchronized HZPositionHelper getInstance(HZCommonHelper commHelper){
        if (instance == null) {
            instance = new HZPositionHelper();
            instance.hzCommonHelper = commHelper;
        }
        return instance;

    }


    //////////////////////////
    public LiquidityPositionDetail getLiquidityPositionDetail(String dataKey) {
        long startTime = System.currentTimeMillis();

        logger.info("start to get position from Hazelcast with key: {}", dataKey);

        if (StringUtils.isBlank(dataKey) ) {
            logger.error("failed to get position from Hazelcast: invalid key or Hazelcast is not initialized, key: {}", dataKey);
            return null;
        }

        String dataValue = HazelcastStore.getJsonMsg(dataKey);

        if (StringUtils.isBlank(dataValue)) {
            logger.error("position from Hazelcast is blank with key: {}", dataKey);
            return null;
        }

        LiquidityPositionDetail positionDetail = GsonUtils.getUpperCamelGson().fromJson(dataValue, LiquidityPositionDetail.class);

        String convertedDataValue = HZUtils.convertJsonStringToSingleLine(dataValue);

        long endTime = System.currentTimeMillis();
        logger.info("get liquidity position from hazelcast done, time: [{}] ms, key: {}, value: {}", endTime - startTime, dataKey, convertedDataValue);

        return positionDetail;

    }

    //Update CP AtomicReference - Position with retry
    public <T> boolean casRetry(String key, UnaryOperator<T> function, Class<T> clazz) {

        try {
            logger.info("cas retry | start to update hazelcast with key: {}", key);
            Boolean result = hzCommonHelper.retryLogic(key, reference -> {
                String newValueStr = null;


                Object oldValueObj = reference.get();

                String oldValue = HZUtils.getStringValueFromAtomRef(key, oldValueObj);


                if (String.class.equals(clazz)) {
                    T oldTypeObj = (T) oldValue;
					
					//Apply business changes to generate a new image
                    T newTypeObj = function.apply(oldTypeObj);
                    if(null == newTypeObj) {return null;}
                    newValueStr = (String) newTypeObj;
                } else {
                    Gson gson = GsonUtils.getUpperCamelGson();
                    T oldTypeObj = gson.fromJson(oldValue, (Type) clazz);
					//Apply business changes to generate a new image
                    T newTypeObj = function.apply(oldTypeObj);
                    if(null == newTypeObj) {return null;}
                    newValueStr = gson.toJson(newTypeObj);
                }

				//Convert String value to HazelCast store format (byte[])
                Object newValueObj = HZUtils.toAtomRefData(key, hzCommonHelper.isCompressedKey(key), newValueStr);

                ///Write to HazelCast Server
                boolean updateResult = reference.compareAndSet(oldValueObj, newValueObj);


                logger.info("cas retry | update atomic reference, key: {}, time: [{}] ms", key,
                        endTimeOfCasUpdate - startTimeOfCasUpdate);
                // end to update atomic reference

                if(updateResult){
                    String convertedNewValueStr = HZUtils.convertJsonStringToSingleLine(newValueStr);
                    logger.info("update success: key = {}, value = {}", key, convertedNewValueStr);
                }

                //If the current operation is successful, return true, otherwise return null
                return updateResult ? true : null;
            }, true);
            return result;
        } catch (Exception ex) {
            logger.error("cas retry | fails to update with this exception: {}", ExceptionUtils.getStackTrace(ex));
            throw new HazelcastStoreException(ex);
        }
    }



}

