package Sales.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.time.LocalDate;

@Data
@AllArgsConstructor
public class SalesByDay {
    private LocalDate transactionDate;
    private Double totalSales;
}
