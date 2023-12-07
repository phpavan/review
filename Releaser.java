
public class Releaser implements AutoCloseable {

    private final Logger logger = LogManager.getLogger(Releaser.class);

    private  ChannelManager channelManager;
    private  Consumer<String, byte[]> consumer;
    private  int port;
    private boolean isRunning = true;
    private final Gson gson = GsonUtils.getUpperCamelGson();

    private EnvParams envParams;

    public Releaser(EnvParams pEnvParams) {
        this.envParams = pEnvParams;
        this.port = envParams.getPort();
        this.channelManager = ChannelManager.getInstance();
    }


	/**
	 * Will consume two Topic: re-init & staging
	 * re-init: subsequent of OutboundAsyncReprocess, sending payments to Bloc/Held Kafka Topic, or to GMO if can be released
     * staging: for reprocess those Held payment (Held can be sent now)	, send to GMO 
	 *
	*/
    public void consume() {

        logger.info("Releaser consumer start....");
        while (isRunning) {
            checkOrReinitKafkaClient();
            try {
                ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                logger.info("releaser poll length is {} ", consumerRecords.count());
                for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
                    if (consumerRecord.value() != null) {
                        MeshMessage meshMessage = Utils.parseRecord(consumerRecord);
						
						//resume its flow (proceed the flow to GMO)
                        Utils.release(meshMessage, channelManager, port);

					}
                    logger.info("offset: {}, partition: {}", consumerRecord.offset(), consumerRecord.partition());
                    consumer.commitSync();
                }
            } catch (Exception e) {
                consumer.close();
                consumer = null;
                logger.error("Releaser throws exception: {}", ExceptionUtils.getStackTrace(e));
            }
        }

    }


    @Override
    public void close() {
        isRunning = false;
        consumer.wakeup();
    }



    private void checkOrReinitKafkaClient(){
        if (consumer == null) {
            logger.info("Releaser consumer is null, will create it");
            consumer = KafkaFactory.getStagingConsumer(envParams);
            logger.info("Releaser consumer : {}",consumer);
        }
    }


}
