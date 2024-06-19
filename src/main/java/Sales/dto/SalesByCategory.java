package Sales.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class SalesByCategory {
    private Timestamp transactionDate;
    private String category;
    private Double totalSales;
}
