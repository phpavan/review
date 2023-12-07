
public class HazelcastStore {

    private static final Logger logger = LogManager.getLogger(HazelcastStore.class);

    private static HazelcastInstance instance = null;
    private static CPSubsystem hazelcastCPSubsystem = null;

    //env var.
    private static String hazelcastName = null;
    private static String hazelcastUrls = null;
    private static String hzCredentialsGoldKey = null;
    private static String hzCredentialsPrincipal = null;


    private static String prefixLock = null;
    private static String prefixImap = null;
    private static String prefixImapCounterparty = null;
    private static String prefixTestKey = null;

    public static String getHazelcastName() {
        return hazelcastName;
    }

    public static String getHazelcastUrls() {
        return hazelcastUrls;
    }

    public static String getHzCredentialsPrincipal() {
        return hzCredentialsPrincipal;
    }

    public static String getHzCredentialsGoldKey() {
        return hzCredentialsGoldKey;
    }


    private static HZPositionHelper hzPositionHelper;
    private static HZSchedulerHelper hzSchedulerHelper;
    private static HZCommonHelper hzCommonHelper;

    private HazelcastStore() {
    }

    public static synchronized void clear() {
        logger.info("clear Hazelcast CPSubsystem instance");
        hazelcastCPSubsystem = null;
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }

        hzSchedulerHelper = null;
        hzPositionHelper = null;
        hzCommonHelper = null;
    }

   
    public static void initializeByEnvVar() {
        logger.info("Initializing Hazelcast CPSubsystem, ");
        String clusterName = HZUtils.loadEnvVariable("HAZELCAST_NAME", String.class);
        String urls = HZUtils.loadEnvVariable("HAZELCAST_URLS", String.class);

        String principal = HZUtils.loadEnvVariable("HAZELCAST_PRINCIPAL", String.class);
        String credentialsGoldKey = HZUtils.loadEnvVariable("HAZELCAST_PASSWORD", String.class);

        prefixImap = HZUtils.loadEnvVariableOrDefault("IMAP_KEY_PREFIX", String.class, "pol.payment-operation");
        prefixLock = HZUtils.loadEnvVariableOrDefault("LOCK_KEY_PREFIX", String.class, "liquidity-lock-");
        prefixImapCounterparty = HZUtils.loadEnvVariableOrDefault("LIQUIDITY_COUNTERPARTY_PREFIX", String.class, "pol.liquidity-counterparty");


        initialize(clusterName, urls, principal, credentialsGoldKey);
    }

    public static void initialize(String clusterName, String urls, String principal, String password) {
        if (StringUtils.isBlank(clusterName) || StringUtils.isBlank(urls)) {
            throw new IllegalArgumentException("Hazelcast cluster name & Urls can not be empty");
        }

        hazelcastName = clusterName;
        hazelcastUrls = urls;
        hzCredentialsPrincipal = principal;
        hzCredentialsGoldKey = password;

        logger.info("Hazelcast Name: {}, Hazelcast URLs: {}", hazelcastName, hazelcastUrls);


        createHazelcastInstance();

        hzCommonHelper = HZCommonHelper.getInstance();
        hzSchedulerHelper = HZSchedulerHelper.getInstance(hzCommonHelper);
        hzPositionHelper = HZPositionHelper.getInstance(hzCommonHelper);

        if("TRUE".equals(HZUtils.loadEnvVariable("COMPRESS_ATOMICREF_VALUE", String.class))){
            logger.info("COMPRESS_ATOMICREF_VALUE=true, converting scheduler/position value to compressed format");
            UpgradeUtils  upgradeUtils = new UpgradeUtils(hzCommonHelper.compressKeyList());
            upgradeUtils.updateStringValueToBayteArray();
        }


    }

    public static synchronized HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    public static synchronized CPSubsystem getHazelcastCPSubsystem() {
        return hazelcastCPSubsystem;
    }

   
    public static IMap<String, String> getDistributedMapForCounterPartyByPostfix(String postfix) {
        String realKey = prefixImapCounterparty + postfix;
        return getHazelcastInstance().getMap(realKey);
    }

    
    public static IMap<String, String> getDistributedMapByPostfix(String postfix) {
        String realKey = prefixImap + postfix;
        return getHazelcastInstance().getMap(realKey);
    }


    //key@CPGroupName
    public static FencedLock getDistributedLockByPostfix(String postfix, String scheduler) {
        String realKey = prefixLock + postfix;
        if (Strings.isNullOrEmpty(scheduler)) {
            throw new IllegalArgumentException("Scheduler for FencedLosk can not be empty.");
        }
        realKey = hzCommonHelper.checkCPObjectName(realKey, StringUtils.toRootUpperCase(scheduler));
        return getHazelcastInstance().getCPSubsystem().getLock(realKey);
    }

    private static void createHazelcastInstance() {

        //create hazelcast client
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(hazelcastName);


        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        if (hazelcastUrls != null) { // only for SonarLint
            networkConfig.setAddresses(Arrays.asList(hazelcastUrls.split(",")));
        }

        if (StringUtils.isNotBlank(hzCredentialsPrincipal) && StringUtils.isNotBlank(hzCredentialsGoldKey)) {
            logger.warn("Both HAZELCAST_PRINCIPAL & HAZELCAST_PASSWORD not null, use security mode ");


            // enable SSL for client connection
            SSLConfig sslConfig = new SSLConfig();
            sslConfig.setEnabled(true);
            networkConfig.setSSLConfig(sslConfig);

            Credentials credentials = new UsernamePasswordCredentials(hzCredentialsPrincipal, hzCredentialsGoldKey);
            clientConfig.setCredentials(credentials);
        }

        // initialize hazelcast client
        instance = HazelcastClient.newHazelcastClient(clientConfig);
        hazelcastCPSubsystem = instance.getCPSubsystem();
        logger.info("hazelcast connection established successfully ");

    }


	//HAZELCAST AtomifReference Read -- Scheduler
    public static LiquidityScheduler getLiquidityScheduler(String dataKey) {
        return hzSchedulerHelper.getLiquidityScheduler(dataKey);
    }

    /**
	 * HAZELCAST AtomifReference Write -- Scheduler
     * @param key           Scheduler key
     * @param funcBizUpdate business function of Scheduler changes
     * @param retry         should retry in case of CAS failure
     * @return
     */
    public static boolean casRetryScheduler(String key, UnaryOperator<LiquidityScheduler> funcBizUpdate, boolean retry) {
        return hzSchedulerHelper.casRetryScheduler(key, funcBizUpdate, retry);
    }

	//HAZELCAST AtomifReference Read -- Position
    public static LiquidityPositionDetail getLiquidityPositionDetail(String dataKey) {
        return hzPositionHelper.getLiquidityPositionDetail(dataKey);
    }

	//HAZELCAST AtomifReference Read -- Position
    public static boolean casRetryPosition(String key, UnaryOperator<LiquidityPositionDetail> function) {
        return casRetryPosition(key, function, LiquidityPositionDetail.class);
    }

	//HAZELCAST AtomifReference Write -- Position
    public static <T> boolean casRetryPosition(String key, UnaryOperator<T> function, Class<T> clazz) {
        return hzPositionHelper.casRetry(key, function, clazz);
    }

	//HAZELCAST AtomifReference Read -- Common ( Both Scheduler & Position)
    public static String getJsonMsg(String key) {
        logger.info("start to get json msg from hazelcast with key: {}", key);

        key = hzCommonHelper.checkCPObjectName(key);
        Object atomicRefObj = HazelcastStore.getHazelcastCPSubsystem().getAtomicReference(key).get();
        String dataValue = HZUtils.getStringValueFromAtomRef(key, atomicRefObj);
        return dataValue;
    }

	//HAZELCAST AtomifReference Write(set) -- Common ( Both Scheduler & Position)
    public static void setJsonMsg(String key, String value) {
        Object data = HZUtils.toAtomRefData(key, hzCommonHelper.isCompressedKey(key), value);
        hazelcastCPSubsystem.getAtomicReference(hzCommonHelper.checkCPObjectName(key)).set(data);
    }

	//HAZELCAST AtomifReference Write(CAS) -- Common ( Both Scheduler & Position)
    public static boolean compareAndSetJsonMsg(String key, String oldValue, String newValue) {
        long startTime = System.currentTimeMillis();

        key = hzCommonHelper.checkCPObjectName(key);
        logger.info("start to compare and set json msg to Hazelcast with key: {}", key);

        Object oldData = HZUtils.toAtomRefData(key, hzCommonHelper.isCompressedKey(key), oldValue);
        Object newData = HZUtils.toAtomRefData(key, hzCommonHelper.isCompressedKey(key), newValue);
        boolean result = hazelcastCPSubsystem.getAtomicReference(key).compareAndSet(oldData, newData);

        long endTime = System.currentTimeMillis();
        logger.info("compare and set json msg to Hazelcast completed, key: {}, time: [{}] ms", key, endTime - startTime);

        return result;
    }

}

