

public class UKAdmi00400102InboundPayloadProcessor implements InboundPayloadProcessor {



    public UKAdmi00400102InboundPayloadProcessor() {
    }

    public Result inboundProcess(JsonObject tableJsonObj) {

	    //update scheduler & position
        if (null != ukAdmi00400102LiquiditySchedulerMarker || null != ukAdmi00400102LiquidityPositionMarker) {
            updateLiquidityData(ukAdmi00400102LiquiditySchedulerMarker, ukAdmi00400102LiquidityPositionMarker, ukAdmi00400102PaymentMessage);
        }
       
	   //some other business
	   
	   //return result
        return Result.success(new HashMap.SimpleEntry<>(Constants.MAP_REPORTING_OBJECT_KEY, ukAdmi00400102AuditTrailPayload),
                new HashMap.SimpleEntry<>(Constants.MAP_PAYMENT_MESSAGE_KEY, ukAdmi00400102PaymentMessage),
                new HashMap.SimpleEntry<>(Constants.MAP_ORIGINAL_PAYMENT_MESSAGE_KEY, ukAdmi00400102OriginalPaymentMessage));
    }


    private void updateLiquidityData(UpdateMarker<LiquidityScheduler> ukAdmi00400102LiquiditySchedulerMarker, UpdateMarker<LiquidityPositionDetail> ukAdmi00400102LiquidityPositionMarker, PaymentMessage ukAdmi00400102PaymentMessage) {
		
        //Update Scheduler
        LiquidityScheduler ukAdmi00400102MarkLiquidityScheduler = null != ukAdmi00400102LiquiditySchedulerMarker ? ukAdmi00400102LiquiditySchedulerMarker.getOperationValue() : null;
        LiquidityPositionDetail ukAdmi00400102MarkLiquidityPosition = null != ukAdmi00400102LiquidityPositionMarker ? ukAdmi00400102LiquidityPositionMarker.getOperationValue() : null;

        if (null != ukAdmi00400102MarkLiquidityScheduler) {
            logger.info("update LiquidityScheduler");
            String schedulerKey = SignalUtils.officeKey(ukAdmi00400102MarkLiquidityScheduler);
			
			/******** Calling Utility CAS method to Update Position ********/
            boolean saveResult = HazelcastStore.casRetryScheduler(
			      Constants.STORAGE_LIQUIDITY_SCHEDULER_PROJECT_NAME + "-" + schedulerKey,
				  
				    //apply changes
                    oldValue-> invokeInternalSettingLiquidityScheduler(ukAdmi00400102PaymentMessage, oldValue, ukAdmi00400102LiquiditySchedulerMarker),
					Boolean.TRUE
			
			);
            BalanceCommitterUtils.checkSaveResultNotReturn(saveResult, null, String.format("UK_INBOUND_Admi00400102, LiquidityScheduler key = %s.", schedulerKey));

		} else {
            logger.info("LiquidityScheduler OperationValue is null, not need to update");
        }
		
		
		//Update Position 
        try {
            if (null != ukAdmi00400102MarkLiquidityPosition) {
                logger.info("update LiquidityPosition");
                String officeKey = SignalUtils.officeKey(ukAdmi00400102MarkLiquidityPosition);
				
				/******** Calling Utility CAS method to Update Position ********/
                boolean saveResult = HazelcastStore.casRetryPosition(
				        Constants.STORAGE_LIQUIDITY_POSITION_PROJECT_NAME + "-" + officeKey,
						
						//apply changes
                        oldValue-> invokeInternalSettingLiquidityPosition(ukAdmi00400102PaymentMessage, oldValue, ukAdmi00400102LiquidityPositionMarker)
				);
				
                BalanceCommitterUtils.checkSaveResultNotReturn(saveResult, "UK_INBOUND_Admi00400102:save successfully!!", String.format("UK_INBOUND_Admi00400102, LiquidityPosition key = %s", officeKey));
            } else {
                logger.info("LiquidityPositionDetail OperationValue is null, not need to update");
            }
        } catch (Exception e) {
            throw Exception 
        }
    }


}
