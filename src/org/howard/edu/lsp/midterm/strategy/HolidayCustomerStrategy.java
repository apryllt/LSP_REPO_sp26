package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for holiday promotion (15% discount).
 * @author Aprille Thomas
 */
public class HolidayCustomerStrategy implements PriceStrategy {

    /**
     * Applies a 15% discount to the original price for holiday promotions.
     *
     * @param price The original price.
     * @return The price after applying a 15% discount.
     */
    @Override
    public double calculate(double price) {
        return price * 0.85;
    }
}
