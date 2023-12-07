
public class PaymentConsumer implements AutoCloseable {


    public EventHandingContext consume(LiquidityTransaction liquidityTransaction) {
		BufferingConsumer<String, byte[]> consumer = new PriorityTopicConsumer(.....);
		ConsumerRecord<String, byte[]> bufferedPaymentRecord = null;

        EventHandingContext eventContext = initNewEventContext(liquidityTransaction);
		
        List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartetList = new ArrayList<>();
		
		
        // pick from priority topic one by one
        Long loopCount = 0l;
        Long produceCount = Long.valueOf(Optional.ofNullable(System.getenv("BATCH_PRODUCE_COUNT")).orElse("500"));
		
        while (consumer.hasNext()) {
            loopCount++;
            bufferedPaymentRecord = consumer.peek();
            String recordKey = bufferedPaymentRecord.key();
           
            PaymentMessageHandlingContext ctx = PaymentMessageHandlingContext.of(eventContext, bufferedPaymentRecord);

            //recordKey : 671070e6-9e73-4d09-84bf-d415c95670b8_ZA-HSBC-SAMOS-ZAR_CLM-11
            String[] recordList = StringUtils.split(recordKey, "_");
            String transactionId = recordList[0];
            String schedulerKey = recordList[1];
            logger.info("start get lock. lock-key:{}, scheduler key :{}", transactionId, schedulerKey);
			
			///1.Validation: check if the payment can be released. if y, add to quartetList
            handleRecord(liquidityTransaction, consumer, bufferedPaymentRecord, quartetList, ctx, transactionId, schedulerKey);

            // failure checking, continue or break
            ///business code here:  break or continue "While Loop"
			
			
			///2.Update: mini batch met, release pending payments
            if(loopCount.equals(produceCount)) {
                logger.info("{} payments are validate success, start to batch release......",produceCount);
                produceMessagesByBatches(consumer, quartetList);
                quartetList.clear();
                loopCount = 0l;
            }
        }
		
        ///3. Update: release pending payments, if any
        produceMessagesByBatches(consumer, quartetList);
        return eventContext;

    }



    private void handleRecord(LiquidityTransaction liquidityTransaction, BufferingConsumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> bufferedPaymentRecord, List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartetList, PaymentMessageHandlingContext ctx, String transactionId, String schedulerKey) {
        FencedLock lock = null;
        String lockStatus = Constants.RESULT_FAILED;
        try {
			
			/** HazelCast FencedLock get lock **/
            lock = ctx.tryLockTransaction(transactionId, schedulerKey);
            /** HazelCast FencedLock try lock **/
            lockStatus = Utils.tryLock(lock, transactionId);
			
            if (Constants.RESULT_SUCCESS.equals(lockStatus)) {
                try {
                    logger.info("TransactionId: {} Acquire the lock successfully", transactionId);
                    handleRecord(liquidityTransaction, consumer, bufferedPaymentRecord, ctx, quartetList);
                } finally {
                    logger.info("TransactionId: {} start unlock ", transactionId);
					
					/** HazelCast FencedLock unlock **/
                    Utils.unlock(lock, transactionId);
                    logger.info("TransactionId: {} unlock success", transactionId);
                }
            } else if (Constants.RESULT_DESTROY.equals(lockStatus)) {
                logger.info("Lock destroyed, skip this payment.");
                ctx.getCheckedResult().setMetAllConditions(true);
                ctx.getEventContext().setDecision(ReleaseDecision.CONTINUE);
                discardPayment(consumer.next(), consumer);
            } else {
                logger.error("Current operation transactionId: {} acquire the lock failed", transactionId);
                ctx.getCheckedResult().setMetAllConditions(false);
            }
        } catch (Exception e) {
            ///proceed those previous in list payments
            produceMessagesByBatches(consumer, quartetList);
			
			/** HazelCast FencedLock unlock **/
            if(lock != null && Constants.RESULT_SUCCESS.equals(lockStatus)) {
                Utils.unlock(lock, transactionId);
            }
            throw e;
        }
    }

	private void handleRecord(LiquidityTransaction liquidityTransaction, BufferingConsumer<String, byte[]> consumer,
                              ConsumerRecord<String, byte[]> bufferedPaymentRecord, final PaymentMessageHandlingContext ctx, List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartetList) {
        logger.info("REQUEST INFORMATION | Payment Consumer Handle Record:");
        // stop attempt to get next element while reached max offset
        if (isMaxOffsetReached(liquidityTransaction, consumer, bufferedPaymentRecord)) {
            return;
        }
        // validate
        businessLogicHandler.getValidatorManager().validate(liquidityTransaction, bufferedPaymentRecord, ctx);

        if (any failure result) {
			//note down
			return ;
        } else {
            logger.info("Validation passed, go to stagePayment step");
            /// Add the pending release payment to quartetList, for further batch release
            stagePayment(consumer.next(), ctx, liquidityTransaction, consumer, bufferedPaymentRecord, quartetList);
        }
    }
	
	 private void stagePayment(ConsumerRecord<String, byte[]> consumerRecord,
                              final PaymentMessageHandlingContext ctx,
                              LiquidityTransaction inputLiquidityTransaction,
                              Consumer<String, byte[]> consumer,
                              ConsumerRecord<String, byte[]> bufferedPaymentRecord,
                              List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartetList) {


        String outboundJson = LiquidityEventFactory.toOutboundJson(inputLiquidityTransaction);

        String consumerRecordKey = consumerRecord.key();

	
        LiquidityTransaction finalOutputLiquidityTransaction = inputLiquidityTransaction;
        String countryCode = finalOutputLiquidityTransaction.getReprocessingResult().getCountryCode();
        logger.info("start updateTombstoneMap");
		
        PaymentMessage paymentMessage = LiquidityConversionUtils.getPaymentMessage(getMeshMessage(bufferedPaymentRecord.value())
                .orElseThrow(() -> new RuntimeException("No message in the mesh message")));
				
		////Add the payment to quartetList, for further batch operation
        Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean> quartet
                = Quartet.with(finalOutputLiquidityTransaction.toJson(), outboundJson, consumerRecord, !ctx.getContextMap().containsKey(VALUE_DATE_FLAG));
        quartetList.add(quartet);
        
		
		////Add payment record to IMAP. to inform that we will update it later,
		/** HazelCast IMAP Write **/
		String tempKey = getTempImapKey(paymentMessage);
        Utils.updateTombstoneMap(paymentMessage,tempKey);

        


    }
	
    private void produceMessagesByBatches(BufferingConsumer<String, byte[]> consumer, List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartetList) {
        ///split into some smaller batch, 10 for example
		Map<Long, List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>>> mappedList =
                quartetList.parallelStream().collect(Collectors.groupingBy(values -> gson.fromJson(values.getValue0(), LiquidityTransaction.class).getReprocessingResult().getLiquidityPosition().getMiniBatchProduceSeq(), Collectors.toList()));
        logger.info("mappedList size: {}", mappedList.size());

		///looping batches
        mappedList.forEach((key, quartets) -> {  //10 payments a batch

            ///Update payment flow by tombstone/producing data. no HazelCast operation involved.
            producer.executeInTransaction(() -> producerMessage(quartets, consumer));

			/** HazelCast IMAP Write. Note down that payment been released **/
            updateTombstoneMap(quartets);

		});
    }

    public void producerMessage(List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartets, BufferingConsumer<String, byte[]> consumer) {
        
		for (int i = 0; i < quartets.size(); i++) {
            Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean> quartet = quartets.get(i);
            String finalOutputLiquidityTransaction = quartet.getValue0();
            String outboundJson = quartet.getValue1();
            ConsumerRecord<String, byte[]> consumerRecord = quartet.getValue2();
            LiquidityTransaction liquidityTransaction = gson.fromJson(finalOutputLiquidityTransaction, LiquidityTransaction.class);
            Long paymentCount = liquidityTransaction.getReprocessingResult().getLiquidityPosition().getOutMsgCount();
            int stagingPartition = partition;
            String countryCode = liquidityTransaction.getReprocessingResult().getCountryCode();
            if (Constants.COUNTRY_CODE_UK.equals(countryCode) && (partition == 0 || partition == 1)) {
                long l = (partition + (paymentCount - 1) * 2) % (2 * STAGING_PARTITION_NUM);
                stagingPartition = (int) l;
            }
            logger.info("StagingPartition: {}", stagingPartition);
			
			///1. Sending payment to Kafka Staging Topic, which for finally send to GMO
            producer.send(envParams.getStagingTopic(), stagingPartition, consumerRecord.key(),
                    consumerRecord.value(), Utils.getHeaders(gson.fromJson(finalOutputLiquidityTransaction, LiquidityTransaction.class)));

            ///2. Sending Position Delta Data to Kafka Topic, for further HazelCast Update in another service
            producer.send(envParams.getLiquidityOutboundTopic(), partition, String.valueOf(partition),
                        outboundJson.getBytes());
            
            ///3. Store necessary data for recovery
            producer.send(envParams.getRecoveryTopic(), partition, String.valueOf(consumerRecord.partition()),
                        finalOutputLiquidityTransaction.getBytes());
				
            ///4. commit offset	of Held topic 			
            producer.commit(consumerRecord.offset(), partition, consumer);
            
            ///5. send tombstone data to original Topic, to clean the data (for KStream)
            producer.send(topicOptional.get().topic(), partition, consumerRecord.key(), null);
            
        }
    }


    public void updateTombstoneMap(List<Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean>> quartets) {
        for (Quartet<String, String, ConsumerRecord<String, byte[]>, Boolean> quartet : quartets) {
            ConsumerRecord<String, byte[]> consumerRecord = quartet.getValue2();
            // produce success and update tombstoneMap
            logger.info("start updateTombstoneMap");
            PaymentMessage paymentMessage = LiquidityConversionUtils.getPaymentMessage(getMeshMessage(consumerRecord.value())
                    .orElseThrow(() -> new RuntimeException("No message in the mesh message")));
            paymentMessage.setTopicKey(consumerRecord.key());//check consumerRecord paymentMessage topicKey is null

            String key = String.join("-",paymentMessage.getCountryCode(),
                    paymentMessage.getOffice(),paymentMessage.getClearingHouse(),paymentMessage.getCurrency());
            logger.info("tombstone map key:{}", key);
			
			/** HazelCast IMAP Write. Note down that payment been released **/
            Utils.updateTombstoneMap(paymentMessage,key);

        }
    }



    private void queuePayment(ConsumerRecord<String, byte[]> consumerRecord,
                              LiquidityTransaction originalLiquidityTransaction, Consumer<String, byte[]> consumer) {
        LiquidityTransaction updatedLiquidityTransaction = originalLiquidityTransaction
                .computeMaxOffsetIfAbsent(queueTopic, offset -> queueConsumer.getMaxOffset());

        if (consumer == null) {
            logger.error(
                    "Not  Support for empty queueConsumer , check if you are trying to buffer the interim topic record in BEST_EFFORT mode");
        } else {
            String consumerRecordKey = consumerRecord.key();
            logger.info("Queueing payment with key: {}; partition: {}; offset: {}, liquidityTransaction: {}",
                    consumerRecordKey, consumerRecord.partition(), consumerRecord.offset(), updatedLiquidityTransaction);
            producer.executeInTransaction(
                    () -> producer.send(envParams.getRecoveryTopic(), partition, String.valueOf(consumerRecord.partition()),
                            updatedLiquidityTransaction.toJson().getBytes()),
                    () -> producer.send(queueTopic, partition, consumerRecord.key(), consumerRecord.value()),
                    () -> producer.commit(consumerRecord.offset(), partition, consumer));
        }
    }




    @Override
    public void close() {
        inboundConsumer.close();
        queueConsumer.close();
    }

  

}
