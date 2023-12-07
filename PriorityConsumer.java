
public class PriorityConsumer implements AutoCloseable, BusinessRequirmentAware {

	private final Logger logger = LogManager.getLogger(PriorityConsumer.class);
	private final TransactionalProducer<String, byte[]> producer;

	private final Consumer<String, byte[]> consumer;
	private final List<PaymentConsumer> paymentsConsumers;
	private final EnvParams envParams;
	private final int partition;
	private boolean isRunning = true;

	private BusinessRequirment businessLogicHandler;

	private UKAutoBlockToHeld ukAutoBlockToHeldService;

	public PriorityConsumer(EnvParams envParams, int partition, TransactionalProducer<String, byte[]> producer,
							BusinessRequirment businessReqquirement ) {
		this.envParams = envParams;
		this.partition = partition;
		this.producer = producer;
		this.businessLogicHandler = businessReqquirement;
		this.consumer = getLiquidityConsumer(envParams, partition);
		// retrieve pending payments from CONSUME_TOPICS(priority topics)
		this.paymentsConsumers = getPaymentConsumers(envParams);

		//init blockToheld service
		ukAutoBlockToHeldService = new UKAutoBlockToHeld();
	}

	public void run() {
		maybeRecover();
		try {
			// resume to pull next one record if any
			consumer.resume(consumer.assignment());
			while (isRunning) {
				ofAll(consumer.poll(Duration.ofSeconds(1)))
						.filter(message -> (message != null) && (message.value() != null)).map((message) ->
								Tuple.of(handleLog(message), message))
								
						 ///1.Parse Kafka record to Java Object LiquidityTransaction
						 ///2.Consume(liquidityTransaction) -- to try releasing payments
						 ///3.releaseLiquidity(liquidityTransaction)  -- unfrozen position(return remaining position) & commit offset of event topic 
						.forEach(tuple -> parse(tuple)
								.onSuccess(liquidityTransaction -> releaseLiquidity(consume(liquidityTransaction)))
								.onFailure(e -> producer.executeInTransaction(
										() -> logger.error("REQUEST ERROR | Commit on error , unknown event message: {}", ExceptionUtils.getStackTrace(e)),
										() -> producer.commit(tuple._2.offset(), partition, consumer),
										() -> SMSAlertUtils.sendAlertText(Constants.SERVICE_NAME, ThreadContext.get(Constants.X_REQUEST_ID),
												CLMAlertID.INCLM102.getLabel(), "Kafka commit failed due to " + e.toString()))));
			}
		} catch (WakeupException e) {
			if (isRunning)
				throw e;
		} catch (Exception e) {
			logger.error("consumer error , unknown event, {}", ExceptionUtils.getStackTrace(e));

		} finally {
			closeGracefully();

		}
	}




	private void maybeRecover() {
		clearTempKey();
		new Recoverer(envParams, partition).maybeRecover().map(this::consume).forEach(this::releaseLiquidity);
	}

	/**
	 * clear the UK imap tmp data, to avoid the dirty imap data block MWS operate the payments
	 */
	public void clearTempKey() {
		String hbukKey = String.join("-", Constants.COUNTRY_CODE_UK,
				Constants.OFFICE_HBUK, Constants.CLEARING_HOUSE, Constants.CCY_GB, Constants.TMP);
		IMap<String, String> hbukMap = HazelcastUtils.getDistributedMap(hbukKey);
		hbukMap.clear();

		String midlKey = String.join("-", Constants.COUNTRY_CODE_UK,
				Constants.OFFICE_MIDL, Constants.CLEARING_HOUSE, Constants.CCY_GB, Constants.TMP);
		IMap<String, String> midlMap = HazelcastUtils.getDistributedMap(midlKey);
		midlMap.clear();

		logger.info("cleared the UK temp imap key......{}, {}", midlKey, hbukKey);
	}


	/**
	 * Consume the event by order
	 *
	 * @param liquidityTransaction the event
	 * @return LiquidityTransaction
	 */
	private LiquidityTransaction consume(LiquidityTransaction liquidityTransaction) {
		//1. First Step : BLOC TO HELD
		//Try to Change Payments from BLOC status to HELD first, if any limitation changed, before reprocess HELD payments
		//BLOC means due to some configuration / amount limit control, payment cant be sent out
		//HELD normally due to insufficient position, should be retry whenever position increased  
		ukAutoBlockToHeldService.process(liquidityTransaction);

		//2. Second Step: Reprocessing Held Payments
		try {

			// looping the priority topics. from High to Low (0 ~ 9)
			for (PaymentConsumer paymentConsumer : paymentsConsumers) {

			    //calling specify topic consumer to proceed 
				EventHandingContext consume = paymentConsumer.consume(liquidityTransaction);
				
				
				// Business logic: Should we move on to next priority topic, if current topic data cant proceed more / done
				liquidityTransaction = consume.getLiquidityTransaction();
				if (consume.getDecision().equals(ReleaseDecision.BREAK)
						&& PRIORITY_ORDER.equals(liquidityTransaction.getPriorityMode())) {
					logger.info("BREAK on PRIORITY_ORDER");
					break;
				}
			}
			logger.info("End of priority Loop");
		} catch (KafkaException e) {
			logger.error("KafkaException when processing transaction: {}, exception: {}", liquidityTransaction, ExceptionUtils.getStackTrace(e));
			throw e;
			//for other exception ,can continue
		} catch (Exception e) {
			logger.error("Exception when processing transaction: {}, exciption: {}", liquidityTransaction, ExceptionUtils.getStackTrace(e));

		}

		return liquidityTransaction;
	}


	
	/**
	 * write null to RecoveryTopic indicate end of txn event write result to output
	 * while complete commited
	 *
	 * @param liquidityTransaction
	 */
	private void releaseLiquidity(LiquidityTransaction liquidityTransaction) {

		if (LiquidityEvent.EventType.BLOCK_TO_HELD_EVENT == liquidityTransaction.getInputEventDetail().getEventType()) {
			logger.info("Block to Held case, directly commit event offset");
			Utils.commitOffsets(liquidityTransaction.getOffset(), partition, consumer);
			return;
		}

		logger.info("Releasing liquidity, liquidityTransaction: {}", liquidityTransaction);
		liquidityTransaction.getReprocessingResult().getLiquidityPosition().setStatusType(StatusType.FINAL);
		String outboundJson = LiquidityEventFactory.toOutboundJson(liquidityTransaction);
		producer.executeInTransaction(
				() -> producer.send(envParams.getRecoveryTopic(), partition, String.valueOf(partition), null),
				() -> producer.send(envParams.getLiquidityOutboundTopic(), partition, String.valueOf(partition),
						outboundJson.getBytes()),
				() -> producer.commit(liquidityTransaction.getOffset(), partition, consumer));
		logger.info("REQUEST SUCCESS | Output Result : {} ", outboundJson);
	}


	@Override
	public void close() {
		isRunning = false;
		consumer.wakeup();
	}

	private void closeGracefully() {
		logger.error("Consumer with assignment: {} closing gracefully", consumer.assignment());
		consumer.close();
		paymentsConsumers.forEach(PaymentConsumer::close);
	}



}
