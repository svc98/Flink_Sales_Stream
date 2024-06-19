package Sales.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SalesByMonth {
    private int year;
    private int month;
    private Double totalSales;
}
