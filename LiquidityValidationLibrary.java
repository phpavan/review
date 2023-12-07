
public class LiquidityValidationLibrary implements Processor {

    @Override
    public ProcessorResult run(final Map<String, String> parameters, final Map<String, String> headers, final List<Payload> payloadsList) throws ProcessorException {
        try {
            //validating headers
            //..........

            //validating payload
            //...................

            return handleGMOInput(headers, inputPayload);
        } finally {
            LogUtils.clearLogThreadContext();
        }
    }

    private ProcessorResult handleGMOInput(final Map<String, String> headers, final Payload inputPayload) throws ProcessorException {
        ProcessorResult result;

        try {

			//Validate key fields of the message
			// ...........
			
            /**
			  ************************************************************
             * Will load HazelCast AtomicReference Value Scheduler & Position for further validation in below code block
			   see loadLiquidityDataFromHazelcast
			 */
			
			LiquidityValidationResult liquidityValidationResult = liquidityValidationForGMO(headers, xmlDoc, paymentAuditTrail);
			
			/*************************************************************
			**/
			
            paymentMessage = liquidityValidationResult.getPaymentMessage();
            paymentMessage.setLastUpdateTime(DateTimeUtils.getSystemTimestamp());

            LiquidityScheduler liquidityScheduler = liquidityValidationResult.getLiquidityScheduler();
            LiquidityPositionDetail positionDetail = liquidityValidationResult.getPositionDetail();

            //Setting routing condition for subsequent flow, according to above validation result 
			// ..............
			
			//return 
            result = ProcessorResultUtils.buildResult(.................)
            return result;
        } catch (
		  //................
        }
    }


    public static LiquidityValidationResult loadLiquidityDataFromHazelcast(PaymentMessage paymentMessage) {
        String dataKey = "";
        String pmsgKey = SignalUtils.officeKey(paymentMessage);
        LiquidityValidationResult liquidityValidationResult = new LiquidityValidationResult();
        try {
            dataKey = "liquidity-scheduler-" + pmsgKey;
            liquidityValidationResult.setLiquidityScheduler(HazelcastStore.getLiquidityScheduler(dataKey));  /**calling utility method to load Scheduler**/ 

            dataKey = "liquidity-position-" + pmsgKey;
            liquidityValidationResult.setPositionDetail(HazelcastStore.getLiquidityPositionDetail(dataKey)); /**calling utility method to load position**/ 
        } catch {
			throw Exception 
        }
        return liquidityValidationResult;
    }
   
}
