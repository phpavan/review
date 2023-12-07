
public class PaymentReprocess implements AutoCloseable, ConsumerRebalanceListener {



    public PaymentReprocess(EnvParams pEnvParams) {
        this.envParams = pEnvParams;
    }

    public void consume() {



        Long pollingLength = Long.valueOf(Optional.ofNullable(System.getenv("POLLING_LENGTH")).orElse("500"));
            while (isRunning) {
                try {


                    checkOrReinitKafkaClient();
					
					
                    long nextPollLength = ReprocessUtils.getNextPollLength(pollingLength, reprocessPreviousHZResponseTime, reprocessCurrentHZResponseTime);
                    pollingLength = nextPollLength;
                    ConsumerRecords<String, byte[]> consumerRecords = paymentReprocessConsumer.poll(Duration.ofMillis(nextPollLength));
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
                    logger.info("Payment reprocess complete to handle current polled records");
                }catch (DataErrorException e) {
                    close();
                    logger.error("Payment reprocess stops due to data error: {}", ExceptionUtils.getStackTrace(e));
                }catch (KafkaException e) {
                    logger.error("Payment reprocess hit kafka exception: {}", ExceptionUtils.getStackTrace(e));
                    handleKafkaException();
                } catch (Exception e) {
                    logger.error("Payment reprocess hit exception: {}", ExceptionUtils.getStackTrace(e));
                    sleep(5000L);
                }
            }

    }

    @Override
    public void close() {
        paymentReprocessConsumer.close();
        paymentReprocessConsumer = null;
        isRunning = false;
    }

    public void handleKafkaException() {
        paymentReprocessConsumer.close();
        paymentReprocessConsumer = null;
        sleep(5000L);
    }

    private void processPositionDeltas(List<ConsumerRecord<String, byte[]>> records) {
	    //consolidate to one delta data
        List<ReprocessingDetail> reprocessingDetails = summarized(records);

        ConsumerRecord<String, byte[]> firstRecord = records.get(0);
        ConsumerRecord<String, byte[]> lastRecord = records.get(records.size() - 1);


        //2 updating position
        String officeKey = LiquidityReprocessUtil.getOfficeKey(reprocessingDetails.get(0));
		
		/*************************************/
		/** Calling utility cas method to update HazelCast Position */
        boolean updatePositionResult = HazelcastStore.casRetryPosition(Utils.getPositionKey(officeKey), oldValue -> {
		
             //update latest offset  keep a consumed offset in HazelCast, so as to avoid duplicate consume (will validate after each poll)
            oldValue.updateOffset(lastRecord.topic() + "_" + lastRecord.partition(), lastRecord.offset());
			
             //calling business service to build up new Position image
            if(Constants.COUNTRY_CODE_UK.equals(oldValue.getCountryCode())) {
                ukReprocessService.calculateBatchReprocessPosition(reprocessingDetails,oldValue);
            } else if(Constants.COUNTRY_CODE_ZA.equals(oldValue.getCountryCode())) {
                zaReprocessService.calculateBatchReprocessPosition(reprocessingDetails,oldValue);
            } else {
                throw new DataErrorException("Unsupported country handling");
            }
            return oldValue;
        });
		
		
		
        //3.1 update succeed
        if (updatePositionResult) {
            logger.info("Payments reprocessing update position successfully");
            //update offset
            Utils.commitCurrentOffset(lastRecord, lastOffsetMap, paymentReprocessConsumer);
        } else {
            //3.2 update failed
            logger.error("failed to update position. seek previous offset:{}", firstRecord.offset());
            paymentReprocessConsumer.seek(new TopicPartition(firstRecord.topic(), firstRecord.partition()), firstRecord.offset());
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
        if (paymentReprocessConsumer == null) {
            logger.info("inboundPositionConsumer is null, will create it");
            paymentReprocessConsumer = KafkaFactory.getOutboundConsumer(envParams);
        }
    }

}
