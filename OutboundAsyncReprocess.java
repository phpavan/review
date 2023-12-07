package com.hsbc.wholesale.payments.pol.liquidity.reprocess;



public class OutboundAsyncReprocess implements AutoCloseable, ConsumerRebalanceListener {


    private Consumer<String, byte[]> consumer;
    private TransactionalProducer<String, byte[]> producer;
    //private final
    private boolean isRunning = true;
    private Map<String, Long> outboundLastOffsetMap = new HashMap<>();

    private  BigDecimal outboundPreviousHZResponseTime = BigDecimal.ZERO;

    private BigDecimal outboundCurrentHZResponseTime = BigDecimal.ZERO;

    private String positionBackUp;

    private String logId;

    private EnvParams envParams;

    private static final ReprocessService ukReprocessService = new UKReprocessServiceImpl();

    private static final ReprocessService zaReprocessService = new ZAReprocessServiceImpl();

    private Gson gson = GsonUtils.getUpperCamelGson();

    public OutboundAsyncReprocess(EnvParams pEnvParams ) {
        this.envParams = pEnvParams;
    }
	
	
    public void consume() {
        String outboundPollingSleepDurationStr = Optional.ofNullable(System.getenv("OUTBOUND_ASYNC_CONSUMER_POLLING_SLEEP_DURATION")).orElse("1000");
        Long outboundPollingSleepDuration = Long.valueOf(outboundPollingSleepDurationStr);
        logger.info("outboundPollingSleepDuration: {}", outboundPollingSleepDuration);
        Long pollingLength = Long.valueOf(Optional.ofNullable(System.getenv("POLLING_LENGTH")).orElse("500"));
        while (isRunning) {
            try {
       
                checkOrReinitKafkaClient();
				
				//dynamic calculate a better poll duration time 
                long nextPollLength = ReprocessUtils.getNextPollLength(pollingLength, outboundPreviousHZResponseTime, outboundCurrentHZResponseTime);
                pollingLength = nextPollLength;
				
                ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(nextPollLength));
                if (consumerRecords.isEmpty()) {
                    continue;
                }
				
            	//Different partitions correspond to different ledger (HazelCast Key)
				//e.g.  UK got two ledger, one is HBUK, another is MIDL
				// Scheduler key will be : liquidity-scheduler-HBUK & liquidity-scheduler-MIDL
				// Position key will be : liquidity-position-HBUK & liquidity-position-MIDL
                for (TopicPartition topicPartition : consumerRecords.partitions()) {
                    List<ConsumerRecord<String, byte[]>> records = consumerRecords.records(topicPartition);
                    if (records.isEmpty()) {
                        logger.info("No records found in the partition: {}", topicPartition.partition());
                        continue;
                    }
                    handlePayments(records);
                }
            } catch (DataErrorException e) {
                close();
                logger.info("Records parsing exception: {}", ExceptionUtils.getStackTrace(e));
            } catch (KafkaException e) {
                shutDownCurrentConsumer();
                logger.info("Kafka throws exception: {}", ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                shutDownCurrentConsumer();
                logger.error("Outbound async consumer exception: {}",  ExceptionUtils.getStackTrace(e));
            }
        }
    }

    private void printlogs() {
        String threadId = UUID.randomUUID().toString();
        MMUtil.bindIdToThread(threadId);
        this.logId = threadId;
    }

	/**
	 * Double check that all the records belongs to the same Scheduler (ledger).. 
	 */
    private void validateScheduler(List<ConsumerRecord<String, byte[]>> records) {

        List<String> keyList = records.stream().map(ConsumerRecord::key).collect(Collectors.toList());
        Map<String, List<String>> mapValue = keyList.stream().collect(Collectors.groupingBy(key ->
                key.split("_")[1]));
        if (mapValue.size() > 1) {
            throw new DataErrorException("Data error, please help check..." + mapValue);
        }
    }

    private void handlePayments(List<ConsumerRecord<String, byte[]>> records) {

        //validation
        validateScheduler(records);

        //e7269b09-a3d8-49f3-be8e-cfc0c50d5b72_ZA-HSBC-SAMOS-ZAR_CLM-11 sample
        ConsumerRecord<String, byte[]> firstRecord = records.get(0);
        String key = firstRecord.key();
        String schedulerKey = key.split("_")[1];

        //recovery if necessary
		//No HazelCast AtomicReference Read/Write required
        records = recoveryAction(records,schedulerKey);

        if(CollectionUtil.isEmpty(records)) {
            logger.info("Records recover finished.....");
            return;
        }

		
		//Parse Kafka byte array record to Java Object 
        Map<MeshMessageOuterClass.MeshMessage, PaymentMessage> transformMap = getPaymentMap(records);
        Map<String, List<PaymentMessage>> paymentMessageMap = new HashMap<>();
        Map<String, List<MeshMessageOuterClass.MeshMessage>> meshMessageMap = new HashMap<>();

        //position update + check the payment status
        ConsumerRecord<String, byte[]> lastRecord = records.get(records.size() - 1);
        boolean handleSuccess = processPayments(paymentMessageMap, meshMessageMap, transformMap,
                schedulerKey, lastRecord);

        if (!handleSuccess) {
            long firstOffset = records.get(0).offset();
            TopicPartition topicPartition = new TopicPartition(firstRecord.topic(), firstRecord.partition());
            logger.info("Position update failed, will start seek and retry, firstOffset: {}", firstOffset);
            consumer.seek(topicPartition, firstOffset);
            return;
        } else {
            outboundLastOffsetMap.put(getCommittedOffsetKey(lastRecord.topic(), lastRecord.partition()), lastRecord.offset());
        }

        //batch update the mesh message
        batchProduce(lastRecord, meshMessageMap);
    }

    private Map<MeshMessageOuterClass.MeshMessage, PaymentMessage> getPaymentMap(List<ConsumerRecord<String, byte[]>> records) {
        Map<MeshMessageOuterClass.MeshMessage, PaymentMessage> meshAndPaymentMessageMap = new LinkedHashMap<>();
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> consumerRecord = records.get(i);
            MeshMessageOuterClass.MeshMessage meshMessage = Utils.parseRecord(consumerRecord);
            PaymentMessage paymentMessage = Utils.getPaymentMessage(meshMessage);

            meshAndPaymentMessageMap.put(meshMessage, paymentMessage);
        }
        return meshAndPaymentMessageMap;
    }

   

    private boolean processPayments(Map<String, List<PaymentMessage>> paymentMessageMap,
                                   Map<String, List<MeshMessageOuterClass.MeshMessage>> meshMessageMap,
                                   Map<MeshMessageOuterClass.MeshMessage, PaymentMessage> calculateMap,
                                   String officeKey,
                                   ConsumerRecord<String, byte[]> lastRecord) {
        try {
			
			/** HazelCast Scheduler Read */
            LiquidityScheduler scheduler = HazelcastStore.getLiquidityScheduler(Constants.LIQUIDITY_SCHEDULER + "-" + officeKey);

            if (scheduler == null) {
                throw new DataErrorException("Hazelcast scheduler is null, officeKey: " +officeKey);
            }
            Set<Map.Entry<MeshMessageOuterClass.MeshMessage, PaymentMessage>> entries = calculateMap.entrySet();

            AtomicInteger retryTimes = new AtomicInteger();
			
				
		   /*************************************/
		   /** Calling utility cas method to update HazelCast Position */
            boolean updateResult = HazelcastStore.casRetryPosition(Constants.LIQUIDITY_POSITION + "-" + officeKey, liquidityPositionSrc -> {
                retryTimes.addAndGet(1);

				//calling the business service to check which payments can be released, which will be Held
				//for those payment being released, we need to deduct position
				//for those payment being Held, we need to calculate held payment count ( by +1, add up to total held payment amount)
				//all of above changes, will be apply to liquidityPositionSrc (HazelCast Position Java Object), in which way, we generate a new Position image
				//and then return the new Position as New Value, so that HazelcastStore.casRetryPosition will Update it to HazelCast Server by CompareAndSet
				//(HazelcastStore.casRetryPosition will store the Old Value before calling the business code)
                if(Constants.COUNTRY_CODE_UK.equals(liquidityPositionSrc.getCountryCode())) {
                    ukReprocessService.handlePayments(meshMessageMap, paymentMessageMap,lastRecord, scheduler, entries, liquidityPositionSrc);
                } else if (Constants.COUNTRY_CODE_ZA.equals(liquidityPositionSrc.getCountryCode())) {
                    zaReprocessService.handlePayments(meshMessageMap, paymentMessageMap,lastRecord, scheduler, entries, liquidityPositionSrc);
                }
                ReprocessUtils.updateIMap(paymentMessageMap, officeKey);

                return liquidityPositionSrc;
            });

            return updateResult;
        } catch (Exception e) {
            logger.error("Throws exception while update position...{}", ExceptionUtils.getStackTrace(e));
            throw new DataErrorException(ExceptionUtils.getStackTrace(e));
        }

    }

    /**
	 *
	 */
    private void batchProduce(ConsumerRecord<String, byte[]> lastRecord,
                              Map<String, List<MeshMessageOuterClass.MeshMessage>> meshMessageMap) {

        List<MeshMessageOuterClass.MeshMessage> blockMessages = meshMessageMap.get(Constants.MSG_STATUS_BLOCK);
        List<MeshMessageOuterClass.MeshMessage> holdMessages = meshMessageMap.get(Constants.MSG_STATUS_HELD);
        List<MeshMessageOuterClass.MeshMessage> releaseMessages = meshMessageMap.get(Constants.MSG_STATUS_SENT);
        //read from scheduler, keep the inbound/outbound the same partition
        String schedulerKey = lastRecord.key().split("_")[1];
        int partition = Utils.getPartitionByOfficeKey(schedulerKey);

		/**
		 * Payment routing condition done, send to a Kafka topic, pending for re-init
		 * re-ini will follow the instruction, send payment to Bloc/Held Queue (Kafka topic) or release (send to GMO)
		 */
        producer.executeInTransaction(() -> {
            long lastOffset = lastRecord.offset();
            outboundLastOffsetMap.put(getCommittedOffsetKey(lastRecord.topic(), lastRecord.partition()), lastOffset);
            producer.commit(lastOffset, lastRecord.partition(), consumer);
            holdMessages.forEach(holdMessage ->
                    producer.send(envParams.getReinitiationTopic(), lastRecord.partition(), holdMessage.getMeshRequestId() + "_hld", holdMessage.toByteArray()));
            releaseMessages.forEach(releaseMessage ->
                    producer.send(envParams.getReinitiationTopic(), lastRecord.partition(), releaseMessage.getMeshRequestId()+ "_rls", releaseMessage.toByteArray()));
            blockMessages.forEach(blockMessage ->
                    producer.send(envParams.getReinitiationTopic(), lastRecord.partition(), blockMessage.getMeshRequestId() + "_blc", blockMessage.toByteArray()));
      
	  
        });
		
      

    }


    private List<ConsumerRecord<String,byte[]>> recoveryAction(List<ConsumerRecord<String,byte[]>> records, String schedulerKey) {

        List<ConsumerRecord<String, byte[]>> validRecords = records.stream().filter(validRecord -> validRecord.value() != null).collect(Collectors.toList());

        List<MeshMessageOuterClass.MeshMessage> blockMessages = new ArrayList<>();
        List<MeshMessageOuterClass.MeshMessage> holdMessages = new ArrayList<>();
        List<MeshMessageOuterClass.MeshMessage> releaseMessages = new ArrayList<>();

        List<ConsumerRecord<String, byte[]>> remainRecords = new ArrayList<>();
        Map<String,String> headers = new HashMap<>();
        logger.info("Imap scheduler key: {}, {}", schedulerKey, schedulerKey.length());
        IMap<String, String> recoverMap = HazelcastStore.getDistributedMapByPostfix(schedulerKey);
        ConsumerRecord<String,byte[]> lastRecord = null;
        for(int i = 0; i < validRecords.size(); i++) {
            ConsumerRecord<String, byte[]> consumerRecord = validRecords.get(i);
            String key = consumerRecord.key();
            logger.info("Outbound async record key: {}", key);
            if(isLastProduceAndCommitFailed(schedulerKey, consumerRecord)) {
                if (!availableRecovery(blockMessages, holdMessages, releaseMessages, headers, recoverMap, consumerRecord)) {
                    continue;
                }
                lastRecord = consumerRecord;
            } else {
                remainRecords.add(consumerRecord);
            }
        }
        //recover
        if(lastRecord != null) {
            batchProduce(lastRecord,ReprocessUtils.getMeshMessageMap(holdMessages,blockMessages,releaseMessages));
        }
        return remainRecords;
    }

    private boolean availableRecovery(List<MeshMessageOuterClass.MeshMessage> blockMessages, List<MeshMessageOuterClass.MeshMessage> holdMessages, List<MeshMessageOuterClass.MeshMessage> releaseMessages, Map<String, String> headers, IMap<String, String> recoverMap, ConsumerRecord<String, byte[]> consumerRecord) {
        MeshMessageOuterClass.MeshMessage meshMessage = Utils.parseRecord(consumerRecord);
        PaymentMessage paymentMessage = Utils.getPaymentMessage(meshMessage);
        String transactionId = paymentMessage.getTransactionId();
        String recoverValue = recoverMap.get(transactionId);
        logger.info("The storage in imap value: {}, transactionId: {}", recoverValue, transactionId);
        if (recoverValue == null) {
            logger.error("Could not find the data in Imap: {}", transactionId);
            return false;
        }

        if(Constants.COUNTRY_CODE_ZA.equals(paymentMessage.getCountryCode())) {
            zaReprocessService.recovery(blockMessages, holdMessages, releaseMessages, headers, meshMessage, paymentMessage, recoverValue);
        } else if (Constants.COUNTRY_CODE_UK.equals(paymentMessage.getCountryCode())) {
            ukReprocessService.recovery(blockMessages, holdMessages, releaseMessages, headers, meshMessage, paymentMessage, recoverValue);
        }
        return true;
    }

	private boolean isLastProduceAndCommitFailed(String schedulerKey, ConsumerRecord<String, byte[]> consumerRecord) {
        int partition = consumerRecord.partition();
        long currentOffset = consumerRecord.offset();
        String topic = consumerRecord.topic();
        logger.info("validating offset.  current consumerRecord topic:{}. partition:{}, offset:{}", topic, partition, currentOffset);
        String positionKey = Constants.LIQUIDITY_POSITION + "-" + schedulerKey;
        Long lastOffset = Utils.getLastOffset(topic, partition, positionKey, outboundLastOffsetMap);
        logger.info("committed offset is : {}", lastOffset);
        if (lastOffset != null && currentOffset <= lastOffset) {
            logger.warn("current offset:{} equals or below to committed offset recorded by HZ. will batch produce and commit offsets", currentOffset);
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        isRunning = false;
        consumer.close();
    }

    private void shutDownCurrentConsumer() {
        consumer.close();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            logger.log(Level.WARN, "Interrupted!", e);
            Thread.currentThread().interrupt();
        }
        consumer = null;
    }

    private void checkOrReinitKafkaClient(){
        if (consumer == null) {
            logger.info("OutboundAsyncReprocess consumer is null, will create it");
            consumer = KafkaFactory.getOutboundAsyncConsumer(envParams);
            logger.info("OutboundAsyncReprocess consumer : {}",consumer);
        }
        if (producer == null) {
            logger.info("OutboundAsyncReprocess producer is null, will create it");
            producer = KafkaFactory.getTransactionalProducer(envParams);
        }
    }



    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.info("partitions revoked. {} ", collection);
        logger.info("cleanup prev recorded topic partition committed offsets if any to let it refresh from Hazelcast server");
        for (TopicPartition topicPartition : collection) {
            this.outboundLastOffsetMap.remove(Utils.getCommittedOffsetKey(topicPartition.topic(), topicPartition.partition()));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.info("partitions assigned. {} ", collection);

        logger.info("cleanup prev recorded topic partition committed offsets if any to let it refresh from Hazelcast server");
        for (TopicPartition topicPartition : collection) {
            this.outboundLastOffsetMap.remove(Utils.getCommittedOffsetKey(topicPartition.topic(), topicPartition.partition()));
        }
    }
}
