
public class HZUtils {

    private static final Logger logger = LogManager.getLogger(HZUtils.class);

    private static final String LIQUIDITY_SCHEDULER = "liquidity-scheduler";
    private static final String LIQUIDITY_POSITION = "liquidity-position";

    private static final Charset CHARSET = StandardCharsets.UTF_8;


    private HZUtils(){}

    public static String getSchedulerKey(String officeKey){
        return LIQUIDITY_SCHEDULER + "-" + officeKey;
    }

    public static String getPositionKey(String officeKey){
        return LIQUIDITY_POSITION + "-" + officeKey;
    }

    ///////////////////////////////////////////////
    public static String convertJsonStringToSingleLine(String str){
        return str == null ? null : str.replaceAll("\\s+"," ");
    }

    // if line separator is \r\n, then convert it to \n
    public static String convertLineSeparatorForDataValue(String key, String dataValue){
        if(StringUtils.isBlank(dataValue)){
            return null;
        }

        if(key.contains(LIQUIDITY_SCHEDULER)){
            LiquidityScheduler liquiditySchedulerSrc = GsonUtils.getUpperCamelGson().fromJson(dataValue, LiquidityScheduler.class);
            return GsonUtils.getUpperCamelGson().toJson(liquiditySchedulerSrc);
        }

        if(key.contains(LIQUIDITY_POSITION)){
            LiquidityPositionDetail liquidityPositionDetailSrc = GsonUtils.getUpperCamelGson().fromJson(dataValue, LiquidityPositionDetail.class);
            return GsonUtils.getUpperCamelGson().toJson(liquidityPositionDetailSrc);
        }

        return dataValue;
    }


    public static String getStringValueFromAtomRef(String dataKey, Object dataObject){

        if(dataObject == null) return null;

        if (dataObject instanceof String) {
            return (String)dataObject;
        }

        if (dataObject instanceof byte[]) {
            return decompressHazelCastValue(dataKey, (byte[])dataObject);
        }

        logger.warn("unexpected data type: {}", dataObject.getClass());
        return String.valueOf(dataObject);

    }

    public static Object toAtomRefData(String key, boolean compress, String value){
        if (compress){
            return compressHazelCastValue(key, value);
        }else{
            return value;
        }
    }

    public static byte[] compressHazelCastValue(String dataKey, String dataValue){
        if (StringUtils.isBlank(dataValue)) {
            logger.error("the string value to compress is empty with key: {}", dataKey);
            return new byte[0];
        }

        try {
            long startTime = System.currentTimeMillis();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(dataValue.getBytes(CHARSET));
            gzip.close();

            byte[] data = out.toByteArray();
            out.close();

            long endTime = System.currentTimeMillis();
            logger.info("compress string value to byte array, key: {}, time: [{}] ms", dataKey, endTime - startTime);

            return data;
        }catch (Exception ex){
            logger.error("compress string to byte array failed with key: {}, exception: {}", dataKey,
                    ExceptionUtils.getStackTrace(ex));
            return new byte[0];
        }
    }

    private  static  String decompressHazelCastValue(String dataKey, byte[] byteArrayValue) {
        if (byteArrayValue == null || byteArrayValue.length == 0) {
            logger.error("the byte array to decompress is empty with key: {}", dataKey);
            return null;
        }

        try{
            long startTime = System.currentTimeMillis();

            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(byteArrayValue));
            String stringValue = IOUtils.toString(gis, CHARSET);

            long endTime = System.currentTimeMillis();
            logger.info("decompress byte array to string, key: {}, time: [{}] ms", dataKey ,endTime - startTime);

            return stringValue;
        }catch (Exception ex){
            logger.error("decompress byte array to string failed with key: {}, exception: {}", dataKey,
                    ExceptionUtils.getStackTrace(ex));
            throw new HazelcastStoreException(ex);
        }
    }

    private static EnvVarLoader envVarLoader = new EnvVarLoader();
    public static <T> T loadEnvVariable(String valueKey, Class<T> clazz) {
        try {
            return envVarLoader.get(clazz, valueKey);
        } catch (Exception ex) {
            logger.warn("Exception occurred while loading var: {}", ex.getMessage());
        }
        return null;
    }

    public static <T> T loadEnvVariableOrDefault(String valueKey, Class<T> clazz, T defaultValue) {
        T result = loadEnvVariable(valueKey, clazz);

        if (result == null) {
            logger.info("failed to load envVariable, set to default. key:{}, value:{}", valueKey, defaultValue);

            result = defaultValue;
        }
        return result;
    }


}

