package com.hsbc.wholesale.payments.pol.liquidity.reprocess;



public class InboundReprocess implements AutoCloseable, ConsumerRebalanceListener {

    private final Logger logger = LogManager.getLogger(InboundReprocess.class);
    private static final Gson gson = GsonUtils.getUpperCamelGson();

    private Consumer<String, byte[]> inboundPositionConsumer;
    private Producer<String, byte[]> reprocessEventProducer;

    private  BigDecimal inboundPreviousHZResponseTime = BigDecimal.ZERO;
    private BigDecimal inboundCurrentHZResponseTime = BigDecimal.ZERO;

    private boolean isRunning = true;
    private Map<String, Long> lastOffsetMap = new HashMap<>();


    private EnvParams envParams;
    public InboundReprocess(EnvParams pEnvParams ) {
        this.envParams = pEnvParams;
    }

    public void consume() {
        logger.info("REQUEST RECEIVED | Inbound Reprocess Consumer starts");

        Long pollingLength = Long.valueOf(Optional.ofNullable(System.getenv("POLLING_LENGTH")).orElse("500"));
            while (isRunning) {
                try {

                    //check Kafka client
                    checkOrReinitKafkaClient();
					

					
					//Polling Inbound Position Delta sent by Balance-committer
                    ConsumerRecords<String, byte[]> consumerRecords = inboundPositionConsumer.poll(pollingLength);
                    logger.info("complete to poll record");
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
						
						//Batch handling position delta data...
                        processPositionDeltas(records);
                    }
                    logger.info("complete to handle current polled records");
                } catch (KafkaException e) {
                    logger.error("inbound reprocess hit kafka exception: {}", ExceptionUtils.getStackTrace(e));
                    handleKafkaException();
                } catch (Exception e) {
                    logger.error("inbound reprocess hit exception: {}", ExceptionUtils.getStackTrace(e));
                    sleep(5000L);
                }
            }

    }

    @Override
    public void close() {
        inboundPositionConsumer.close();
        inboundPositionConsumer = null;
        isRunning = false;
    }

    public void handleKafkaException() {
        inboundPositionConsumer.close();
        inboundPositionConsumer = null;
        sleep(5000L);
    }

	/**
	 * @records  Inbound Position Delta. Nothing todo with Inbound payment flow (may actually has been sent to GMO and then outter system)
	*/
    private void processPositionDeltas(List<ConsumerRecord<String, byte[]>> records) {
        logger.info("start to handle position delta by batch, partition: {}, records size:{}", records.get(0).partition(), records.size());

		//consolidate to one delta data
        LiquidityPositionDetail sumPositionDelta = summarized(records);

        ConsumerRecord<String, byte[]> firstRecord = records.get(0);
        ConsumerRecord<String, byte[]> lastRecord = records.get(records.size() - 1);
        //1 no position changed required
        if (sumPositionDelta == null) {
            //update offset
            logger.info("no position changes required, commit offset. offset:{} ", lastRecord.offset());
            Utils.commitCurrentOffset(lastRecord, lastOffsetMap, inboundPositionConsumer);
            return;
        }

        //2 updating position
        String officeKey = SignalUtils.officeKey(sumPositionDelta);

        logger.info("will update hz position.");
        AtomicInteger retryTimes = new AtomicInteger(0);
		
		/*************************************/
		/** Calling utility cas method to update HazelCast Position */
        boolean updatePositionResult = HazelcastStore.casRetryPosition(Utils.getPositionKey(officeKey), oldValue -> {
            retryTimes.addAndGet(1);
            //update latest offset  keep a consumed offset in HazelCast, so as to avoid duplicate consume (will validate after each poll)
            oldValue.updateOffset(lastRecord.topic() + "_" + lastRecord.partition(), lastRecord.offset());

            //update Position to HazelCast
            DeltaPositionUtils.calculateTheWholePosition(sumPositionDelta, oldValue);

            return oldValue;
        });
		/************************************/

       
        //3.1 update succeed
        if (updatePositionResult) {
            logger.info("update position successfully");
            //update offset
            Utils.commitCurrentOffset(lastRecord, lastOffsetMap, inboundPositionConsumer);
            String requestId = getRequestId(firstRecord, lastRecord);
			
			/*************************************/
		    /** Will Frozen Position (HazelCast Update )for further reprocess process  */
            triggerAutoReprocessEvent(officeKey, requestId, sumPositionDelta);
	        /************************************/

        } else {
            //3.2 update failed
            logger.error("failed to update position. seek previous offset:{}", firstRecord.offset());
            inboundPositionConsumer.seek(new TopicPartition(firstRecord.topic(), firstRecord.partition()), firstRecord.offset());
        }

        logger.info("complete to handle position delta by batch, partition: {}, records size:{}", records.get(0).partition(), records.size());

    }


    private LiquidityPositionDetail summarized(List<ConsumerRecord<String, byte[]>> records){
        logger.info("start to summarize records, partition: {}, records size:{}", records.get(0).partition(), records.size());
        LiquidityPositionDetail sumPositionDelta = null;

        for (ConsumerRecord<String, byte[]> record : records) {
            String key = record.key();
            logger.info("handling position delta with record key: {}", key);

            //1. empty value filter
            if (!Optional.ofNullable(record.value()).isPresent()) {
                logger.info("position delta is empty, skip this record, record key is {}", key);
                continue;
            }

            //2. parse record
            String positionDeltaStr = new String(record.value());
            LiquidityPositionDetail positionDelta;
            try {
                positionDelta = gson.fromJson(positionDeltaStr, LiquidityPositionDetail.class);
            } catch (Exception e) {
                logger.error("Failed to deserialized, skip this record, record key: {}, exception: {}",
                        key, ExceptionUtils.getStackTrace(e));
                continue;
            }

            //3. validate offset (duplicate consume checking)
            String officeKey = SignalUtils.officeKey(positionDelta);
            String positionKey = Utils.getPositionKey(officeKey);
            Long lastCommittedOffset = Utils.getLastOffset(record.topic(), record.partition(), positionKey, lastOffsetMap);

            long currentOffset = record.offset();

            //&& currentOffset + MaxPossibleBatchSize > lastCommittedOffset  to avoid offset reset case????
            if (lastCommittedOffset != null && currentOffset <= lastCommittedOffset) {
                logger.warn("recordKey {}, the current offset {} <= committed offset recorded by HZ, skip this record.",
                        key, currentOffset);
                continue;
            }

            logger.info("position delta is valid, record key: {}", key);

            //4. summarized
            if(sumPositionDelta == null) {
                sumPositionDelta = new LiquidityPositionDetail();
            }
            DeltaPositionUtils.calculateTheWholePosition(positionDelta, sumPositionDelta);

        }

        logger.info("complete to summarize records, partition: {}, records size:{}", records.get(0).partition(), records.size());

        return sumPositionDelta;
    }


    public void triggerAutoReprocessEvent(String officeKey, String requestId, LiquidityPositionDetail sumPositionDelta ) {
        if(sumPositionDelta.getLiquidityPosition() == null || sumPositionDelta.getLiquidityPosition().compareTo(BigDecimal.ZERO) <= 0){
            logger.info("liquidityPosition of sumPositionDelta is not greater than zero, will not trigger auto-release event");
            return;
        }

        String operator = "System";
		
		/** Will Frozen Position (HazelCast Update )for further reprocess process  */
        if (Constants.COUNTRY_CODE_UK.equals(sumPositionDelta.getCountryCode())) {
            //trigger blockToHeld & reprocess event
            List<String> counterPartyBicList = filterBlockToHeldCounterParty(sumPositionDelta);
            logger.info("counter party bic list for bloc to held: {}", counterPartyBicList);
            ReprocessHelper.ukReprocessBlocAndHeldPayments(requestId, operator, officeKey, counterPartyBicList);

        } else if (Constants.COUNTRY_CODE_ZA.equals(sumPositionDelta.getCountryCode())){
            //trigger reprocess event
            ReprocessHelper.zaReprocessPayments(requestId, operator, officeKey);
        }

    }



    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.info("partitions revoked. {} ", collection);
        logger.info("cleanup prev recorded topic partition committed offsets if any to let it refresh from Hazelcast server");
        for (TopicPartition topicPartition : collection) {
            this.lastOffsetMap.remove(Utils.getCommittedOffsetKey(topicPartition.topic(), topicPartition.partition()));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.info("partitions assigned. {} ", collection);

        logger.info("cleanup prev recorded topic partition committed offsets if any to let it refresh from Hazelcast server");
        for (TopicPartition topicPartition : collection) {
            this.lastOffsetMap.remove(Utils.getCommittedOffsetKey(topicPartition.topic(), topicPartition.partition()));
        }
    }

    private void checkOrReinitKafkaClient(){
        if (inboundPositionConsumer == null) {
            logger.info("inboundPositionConsumer is null, will create it");
            inboundPositionConsumer = KafkaFactory.getInboundAsyncConsumer(envParams);
        }

        if (reprocessEventProducer == null) {
            logger.info("reprocessEventProducer is null, will create it");
            reprocessEventProducer = KafkaFactory.getTransactionalProducer(envParams);
        }
    }


    private String getRequestId(ConsumerRecord<String, byte[]> firstRecord, ConsumerRecord<String, byte[]> lastRecord){
        String firstTxnId = firstRecord.key().split("_")[0];
        String lastTxnId = lastRecord.key().split("_")[0];
        return "INBOUND_ASYNC_" + firstTxnId + "_" + lastTxnId;
    }

    private void sleep(Long milliSeconds){
        logger.info("will sleep {} milliSeconds,", milliSeconds);
        try {
            Thread.sleep(milliSeconds);
        } catch (InterruptedException ex) {
            logger.warn("thread sleep hit exception {}", ExceptionUtils.getStackTrace(ex));
            Thread.currentThread().interrupt();
        }
    }
}
