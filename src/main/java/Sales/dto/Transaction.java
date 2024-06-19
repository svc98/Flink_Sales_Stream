package Sales.dto;

import lombok.Data;
import java.sql.Timestamp;

@Data
public class Transaction {
    private String transactionId;
    private int productId;
    private String productName;
    private String productBrand;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private double totalAmount;
    private String currency;
    private String paymentMethod;
    private String customerId;
    private Timestamp transactionDate;
}