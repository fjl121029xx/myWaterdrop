package io.github.interestinglab.waterdrop;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

/**
 * @Author jiaquanyu
 * @Date 2019/6/26 10:35 AM
 */
public class SparkSturctTypeUtilTest {

    @Test
    public void getStructTypeTest() {

        JSONObject jsonObject = JSON.parseObject("{\"deviceName\" : \"string\", \"foodRealPrice\" : \"decimal\", \"foodPayPrice\" : \"decimal\", \"sendBy\" : \"string\", \"allFoodRemark\" : \"string\", \"foodSubType\" : \"string\", \"areaName\" : \"string\", \"foodSubjectCode\" : \"string\", \"isDiscount\" : \"integer\", \"serveConfirmTime\" : \"long\", \"setFoodCategoryName\" : \"string\", \"salesCommission\" : \"decimal\", \"makeEndNumber\" : \"decimal\", \"startTime\" : \"long\", \"departmentKeyLst\" : \"string\", \"foodCode\" : \"string\", \"actionTime\" : \"long\", \"shopID\" : \"long\", \"foodCategorySortIndex\" : \"integer\", \"saasOrderKey\" : \"string\", \"makeCallCount\" : \"integer\", \"isDiscountDefault\" : \"integer\", \"foodRealAmount\" : \"decimal\", \"itemKey\" : \"string\", \"createTime\" : \"long\", \"readyNumber\" : \"decimal\", \"foodTaste\" : \"string\", \"shiftName\" : \"string\", \"parentFoodFromItemKey\" : \"string\", \"foodLastCancelNumber\" : \"decimal\", \"foodKey\" : \"string\", \"itemID\" : \"string\", \"taxRate\" : \"decimal\", \"foodPriceAmount\" : \"decimal\", \"foodEstimateCost\" : \"decimal\", \"cancelReason\" : \"string\", \"orderSubType\" : \"integer\", \"orderStatus\" : \"integer\", \"groupID\" : \"long\", \"cancelBy\" : \"string\", \"timeNameCheckout\" : \"string\", \"setFoodRemark\" : \"string\", \"foodProPrice\" : \"decimal\", \"makeStatus\" : \"integer\", \"foodRemark\" : \"string\", \"foodCategoryName\" : \"string\", \"isNeedConfirmFoodNumber\" : \"integer\", \"isSFDetail\" : \"integer\", \"clientType\" : \"string\", \"promotionIDLst\" : \"string\", \"foodVipPrice\" : \"decimal\", \"makeBy\" : \"string\", \"modifyTime\" : \"long\", \"isBatching\" : \"integer\", \"tableName\" : \"string\", \"sendTime\" : \"long\", \"sendReason\" : \"string\", \"brandID\" : \"long\", \"orderBy\" : \"string\", \"foodPractice\" : \"string\", \"foodNumber\" : \"decimal\", \"transmitNumber\" : \"decimal\", \"foodCancelNumber\" : \"decimal\", \"modifyBy\" : \"string\", \"foodSubjectName\" : \"string\", \"unitKey\" : \"string\", \"foodCategoryKey\" : \"string\", \"serverMAC\" : \"string\", \"isSetFood\" : \"integer\", \"makeEndfoodNumber\" : \"decimal\", \"categoryDiscountRate\" : \"decimal\", \"shopName\" : \"string\", \"unit\" : \"string\", \"foodSubjectKey\" : \"string\", \"foodDiscountRate\" : \"decimal\", \"departmentKeyOne\" : \"string\", \"foodPayPriceReal\" : \"decimal\", \"cancelTime\" : \"long\", \"modifyReason\" : \"string\", \"makeEndTime\" : \"long\", \"unitAdjuvantNumber\" : \"decimal\", \"makeStartTime\" : \"long\", \"reportDate\" : \"long\", \"foodSendNumber\" : \"decimal\", \"checkoutTime\" : \"long\", \"setFoodName\" : \"string\", \"action\" : \"integer\", \"unitAdjuvant\" : \"string\", \"foodName\" : \"string\", \"actionBatchNo\" : \"string\", \"serveConfirmBy\" : \"string\", \"isTempFood\" : \"integer\", \"printStatus\" : \"integer\"}\n");
        System.out.println(SparkSturctTypeUtil.getStructType(new StructType(), jsonObject));
    }
}
