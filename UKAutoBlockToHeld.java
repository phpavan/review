
public class UKAutoBlockToHeld {


    public void process(LiquidityTransaction liquidityTransaction){
        logger.info("UK BLock to Held start");

        String officeKey = Utils.getOfficeKey(liquidityTransaction);
        List<String> counterPartyList = liquidityTransaction.getInputEventDetail().getBlockToHeldCounterPartyList();

		

		/** HazelCast AtomicReference Read -- Scheduler **/
        LiquidityScheduler liquidityScheduler = Utils.getLiquidityScheduler(officeKey);
		
        /** HazelCast AtomicReference Read -- Position **/
        LiquidityPositionDetail positionDetail = Utils.getLiquidityPositionDetail(officeKey);

        List<String> availableCounterPartyList = filterAvailableCounterPartyList(liquidityScheduler, positionDetail);
		
        String operator = "";
        doBlockToHeld(liquidityScheduler, positionDetail, availableCounterPartyList, operator);
    }

    private void doBlockToHeld(LiquidityScheduler liquidityScheduler, LiquidityPositionDetail positionDetail, List<String> counterPartyList,
                               String operator) {

		///Looping every counter party, find the payments belong to the counter party and check if can be changed from block to held
        counterPartyList.parallelStream().forEach(counterPartyBic -> {
            logger.info("start to do block to held for counter party: {}", counterPartyBic);

            Counterparty counterpartySch = liquidityScheduler.getCounterpartyMap().get(counterPartyBic);
            CounterpartyPosition counterPartyPosition = positionDetail.getCounterpartyPositionMap().get(counterPartyBic);

            BigDecimal bilateralBalance = counterpartySch.getIntraLimit().add(counterPartyPosition.getIntradayPosition());

            logger.info("counter party: {}, bilateralBalance : {}", counterPartyBic, bilateralBalance);

            //1. fetch block payments From Kafka + check if it can be Changed (business logic)
            List<PaymentMessage> paymentList = getPaymentMessageList(liquidityScheduler, counterPartyBic, bilateralBalance);
       

            //2. change status from block to held
            String schedulerId =  liquidityScheduler.getSchedulerId();
            String officeKey = SignalUtils.officeKey(liquidityScheduler);
            int paymentPartition = Integer.valueOf(schedulerId.split("-")[1]);


            paymentList.forEach(paymentMessage -> {
                        paymentBlockToHeld(paymentMessage, paymentPartition, officeKey, schedulerId, operator);
                    }
            );
        
        });
    }

    private void paymentBlockToHeld(PaymentMessage paymentMessage, int paymentPartition, String officeKey, String schedulerId, String operator){
        String transactionId = paymentMessage.getTransactionId();
        logger.info("start to do payment block to held, transaction id: {}", transactionId);

        FencedLock lock = null;
        String lockStatus = null;

        try{
            //step 1: lock payment By TransactionId
			/** HazelCast FencedLock **/
            lock = HazelcastStore.getDistributedLockByPostfix(transactionId, officeKey);
            lockStatus = Utils.tryLock(lock, transactionId);
            if(!Constants.RESULT_SUCCESS.equals(lockStatus)){
                logger.info("lock payment failed, result: {}, transaction id: {}", lockStatus, transactionId);
                return;
            }

            // step 2: check payment in Imap ( in progress or done by any other concurrent thread ?)
            String paymentInImapResult = isPaymentInImap(paymentMessage);
            if(!Constants.RESULT_VALID.equals(paymentInImapResult)){
                logger.warn("checked payment already in IMap or tmp IMap, result: {}, transaction id: {}", paymentInImapResult , transactionId);
                return;
            }

            // step 3: send held message to kafka, and tombstone block message synchronously
            syncBlockToHeld(paymentMessage, paymentPartition, officeKey, schedulerId, operator);

            //step 4: add payment to imap (Note down the latest state of the payment )
            String addPaymentToIMapResult = addPaymentToIMap(officeKey, paymentMessage);
            if(!Constants.RESULT_SUCCESS.equals(addPaymentToIMapResult)){
                logger.error("add payment to IMap failed, result: {}, transaction id: {}", addPaymentToIMapResult, transactionId);
            }
        }catch (Exception e){
            logger.warn("hit exception when payment block to held, transaction id: {}, exception: {}",
                    transactionId, ExceptionUtils.getStackTrace(e));
        } finally {
            //step 5: unlock payment
            if(lock != null && Constants.RESULT_SUCCESS.equals(lockStatus)){
                String unlockResult  = Utils.unlock(lock, transactionId);
                if(!Constants.RESULT_SUCCESS.equals(unlockResult)){
                    logger.error("unlock payment failed, result: {}, transaction id: {}", unlockResult, transactionId);
                }
            }
        }

        logger.info("complete to do payment block to held, transaction id: {}", transactionId);
    }

    private String isPaymentInImap(PaymentMessage paymentMessage) {
        String transactionId = paymentMessage.getTransactionId();
        logger.info("start to check payment in Imap, transaction id: {}", transactionId);

        String mapKey = String.join("-", paymentMessage.getCountryCode(), paymentMessage.getOffice(), paymentMessage.getClearingHouse(), paymentMessage.getCurrency());
        String recordKey = String.join("_", paymentMessage.getTopicKey(), String.valueOf(paymentMessage.getVersion()));

		/** HazelCast IMAP Read **/
        IMap<String, String> tmpMap = HazelcastStore.getDistributedMapByPostfix(mapKey + "-TMP");
        if (tmpMap.containsKey(recordKey)) {
            return Constants.RESULT_IN_TMP_IMAP;
        }
		
        /** HazelCast IMAP Read **/
        IMap<String, String> map = HazelcastStore.getDistributedMapByPostfix(mapKey);
        if (map.containsKey(recordKey)) {
            return Constants.RESULT_IN_IMAP;
        }

        return Constants.RESULT_VALID;
    }
    private String addPaymentToIMap(String officeKey, PaymentMessage paymentMessage) {
        String transactionId = paymentMessage.getTransactionId();
        logger.info("start to add payment to Imap, transaction id: {}", transactionId);
        //prepare key
        String recordKey = String.join("_", paymentMessage.getTopicKey(), String.valueOf(paymentMessage.getVersion()));

        IMap<String, String> map = HazelcastStore.getDistributedMapByPostfix(officeKey);

        String utcTimeZone = TOMBSTONE_DATE_TIME_FORMATTER.format(Utils.getUTCTimeZone());
		
		/** HazelCast IMAP Write **/
        String tombstoneTime = map.putIfAbsent(recordKey, utcTimeZone, 7, TimeUnit.DAYS);
        //If tombstoneTime is not null, it means that the payment has already been operated and cannot continue to be executed currently
        if (StringUtils.isNotBlank(tombstoneTime)) {
            return Constants.RESULT_ADD_TO_IMAP_FAILED;
        }

        return Constants.RESULT_SUCCESS;
    }

 

    private void syncBlockToHeld(PaymentMessage paymentMessage,int paymentPartition, String officeKey, String schedulerId, String operator){
        logger.info("start to do payment block to held synchronously, transaction id: {}", paymentMessage.getTransactionId());

        transactionalProducer.executeInTransaction(() ->{
            //1. fetch MeshMessage
            String partitionKey = officeKey + "_" + schedulerId;
			//new state of the payment 
            MeshMessageOuterClass.MeshMessage newMeshMessage = buildNewMeshMessage(paymentMessage, partitionKey, operator);

            String priorityTopic =  Constants.PRIORITY_LIQUIDITY_QUEUE_PREFIX + paymentMessage.getPriority();
            String blocTopic = paymentMessage.getTopic();
            String topicKey = paymentMessage.getTopicKey();
            logger.info("priorityTopic: {}, blocTopic: {}, topicKey: {}", priorityTopic, blocTopic, topicKey);

			
			//////Moving from Block to Held
			//Produce Latest Payment State to Held Topic 
            transactionalProducer.send(new ProducerRecord<>(priorityTopic, paymentPartition, topicKey, newMeshMessage.toByteArray()));
            //send a tombstone message to delete block message
            transactionalProducer.send(new ProducerRecord<>(blocTopic, paymentPartition, topicKey, null));
        });
    }

}
