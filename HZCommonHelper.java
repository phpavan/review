
public class HZCommonHelper {

    private static final Logger logger = LogManager.getLogger(HZCommonHelper.class);

    //retry related env param
    private Integer maxRetryCount;
    //ms
    private Integer minSleepTime;
    private Integer maxSleepTime;
    private List<String> byteArrayHzKeys;

	/**
	  Config of consumer (Inward/Outward/Reinit)
	      HZ_RETRY_MAX_COUNT: 28
          HZ_RETRY_MIN_SLEEP_TIME: 200
          HZ_RETRY_MAX_SLEEP_TIME: 6000
		  
		  
     Config of other service (MWS operations which from User )
		  HZ_RETRY_MAX_COUNT: 15
          HZ_RETRY_MIN_SLEEP_TIME: 40
          HZ_RETRY_MAX_SLEEP_TIME: 1500
	*/
    private HZCommonHelper() {
        maxRetryCount = HZUtils.loadEnvVariableOrDefault("HZ_RETRY_MAX_COUNT", Integer.class, 3);
        minSleepTime = HZUtils.loadEnvVariableOrDefault("HZ_RETRY_MIN_SLEEP_TIME", Integer.class, 100);
        maxSleepTime = HZUtils.loadEnvVariableOrDefault("HZ_RETRY_MAX_SLEEP_TIME", Integer.class, 3000);

        logger.info("HZ retry related param. maxRetryCount:{}, minSleepTime:{}, maxSleepTime:{}", maxRetryCount, minSleepTime, maxSleepTime);

        //compress atomic reference keys
        initCompressKeyList();

    }

    private static HZCommonHelper instance;

    public static synchronized HZCommonHelper getInstance() {
        if (instance == null) {
            instance = new HZCommonHelper();
        }

        return instance;

    }

    private void initCompressKeyList() {
		//Currently, ALL the atomic reference value will need to be compressed
        String filename = "ByteArrayKeys_MainFlow.json";

        try {
            String byteArrayKeyInfo = IOUtils.resourceToString(filename, StandardCharsets.UTF_8, HazelcastStore.class.getClassLoader());
            ByteArrayKeyDetail byteArrayKeyDetail = GsonUtils.getUpperCamelGson().fromJson(byteArrayKeyInfo, ByteArrayKeyDetail.class);

            byteArrayHzKeys = byteArrayKeyDetail.getByteArrayKeys();
        } catch (IOException e) {
            logger.error("failed to load atomRef compression keys info with exception {}", ExceptionUtils.getStackTrace(e));
            throw new HazelcastStoreException(e);
        }

    }

    public boolean isCompressedKey(String key) {
        return byteArrayHzKeys.contains(key.split("@")[0]);
    }

    public List<String> compressKeyList() {
        return byteArrayHzKeys;
    }

    public boolean retryLogic(String key, Function<IAtomicReference<Object>, Boolean> funcUpdateAtomicRef, boolean retry) {
        int retryCount = 1;
        int totalSleepTime = 0;
        int sleepTime = minSleepTime;

        long startTime = System.currentTimeMillis();
        long lastEndTime = 0L;

        key = checkCPObjectName(key);
        IAtomicReference<Object> reference = HazelcastStore.getHazelcastCPSubsystem().getAtomicReference(key);

        Boolean result;
        do {
            //1. try to update
            result = funcUpdateAtomicRef.apply(reference);
            long endTimeOfExistingOperation = System.currentTimeMillis();
            long thisOptTime = endTimeOfExistingOperation - lastEndTime;

            logger.info("operation cost: {}ms", thisOptTime);

            //2.1. if result is true, final updated result is success
            if (null != result) {
                logger.info("operation success | key: {}, retryCount: {}, cost: {}ms", key, retryCount, endTimeOfExistingOperation - startTime);
                return result;
            }

            //2.2 update failed
            logger.info("operation failed | key: {}, retryCount: {},  cost: {}ms", key, retryCount, endTimeOfExistingOperation - startTime);

            if (!retry || retryCount >= maxRetryCount) {
                logger.warn("operation failed | maximum retries ({} times) exceeded. key = {}, total sleep time(ms): {}, total cost: {}ms",
                        maxRetryCount, key, totalSleepTime, endTimeOfExistingOperation - startTime);
                return false;
            }

            //3. preparation for next try
            sleepPeriod(sleepTime);
            totalSleepTime += sleepTime;
            sleepTime = acquireSleepTime(sleepTime);
            retryCount++;
            lastEndTime = System.currentTimeMillis();

        } while (true);
    }

    private void sleepPeriod(int sleepTime) {
        try {
            logger.info("sleep {} ms to avoid hit.", sleepTime);
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            logger.warn("failed to sleep.", e);
            Thread.currentThread().interrupt();
        }
    }

    private int acquireSleepTime(int minSleepTime) {
        int sleepTime;
        Integer hzMaxSleepTime = maxSleepTime;
        if (null == hzMaxSleepTime) {
            hzMaxSleepTime = 2000;
        }
        if (minSleepTime * 2 < hzMaxSleepTime) {
            sleepTime = minSleepTime * 2;
        } else {
            sleepTime = hzMaxSleepTime;
        }
        return sleepTime;
    }

	////////// Below For Multiple CP Group Members //////////////////
    /*
     * Check CP object key.
     *
     *  The configuration map can be null or empty, the key will be not changed.
     *
     *  Else the map must contain the Scheduler mapping, will throw if missing.
     *
     */
    protected static String checkCPObject(String key, Function<IMap<String, String>, String> func) {

        if (key.contains("@")) {
            logger.info("CP system object key: {}, already contains CP group suffix.", key);
            return key;
        }

        var map = getSchedulerCPGroupMap();
        if (map == null || map.isEmpty()) {
            logger.info("No CP group suffix configured. CP system object key: {} will be used without changes.", key);
            return key;
        }

        return func.apply(map);

    }

    public static String checkCPObjectName(String key) {

        return checkCPObject(key, m -> {
            var mk = m.keySet().stream().filter(key::contains).collect(Collectors.toList());
            if (mk.size() != 1) {
                logger.error("CP system object with key: {}, cannot be found or with multiple matched value in IMap: {} ", key, String.join(";", m.entrySet().stream().map(e -> e.getKey() + " = " + e.getValue()).toArray(String[]::new)));
                throw new CLMSystemException(String.format("Failed to load CP group name from IMap %s by key: %s", Constants.HAZELCAST_SCHEDULER_CP_GROUP_MAP, key), null);
            }

            var group = m.get(mk.iterator().next());
            if (null == group || group.isBlank() || group.isEmpty()) {
                logger.info("Blank or Empty CP group suffix configured. CP system object key: {} will be used without changes.", key);
                return key;
            } else {
                var newKey = key + "@" + group;
                logger.info("Revised CP system object key: {}, with CP group suffix as : {} ", key, newKey);
                return newKey;
            }

        });
    }

    public static String checkCPObjectName(String key, String scheduler) {

        return checkCPObject(key, map -> {

            if (!map.containsKey(scheduler)) {
                logger.error("CP system object with scheduler: {}, cannot be found in IMap: {} ", scheduler, map.toString());
                throw new CLMSystemException(String.format("Failed to load CP group name from IMap %s by scheduler: %s", Constants.HAZELCAST_SCHEDULER_CP_GROUP_MAP, scheduler), null);
            }

            var group = map.get(scheduler);
            if (null == group || group.isBlank() || group.isEmpty()) {
                logger.info("Blank or Empty CP group suffix configured. CP system object key: {} will be used without changes.", key);
                return key;
            } else {
                var newKey = key + "@" + group;
                logger.info("Revised CP system object key: {}, with CP group suffix as : {} ", key, newKey);
                return newKey;
            }

        });

    }

    public static IMap<String, String> getSchedulerCPGroupMap() {
        IMap<String, String> map = null;
        try {
            map = HazelcastStore.getHazelcastInstance().getMap(Constants.HAZELCAST_SCHEDULER_CP_GROUP_MAP);
        } catch (java.security.AccessControlException e) {
            logger.info("Map [{}] does not exist in this environment. error: {}", Constants.HAZELCAST_SCHEDULER_CP_GROUP_MAP, e);
            map = null;
        }
        return map;
    }
	////////// Above For Multiple CP Group Members //////////////////

}

